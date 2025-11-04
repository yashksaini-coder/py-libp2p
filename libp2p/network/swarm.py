from collections.abc import (
    Awaitable,
    Callable,
)
import logging
import random
from typing import Any, cast

from multiaddr import (
    Multiaddr,
)
import trio

from libp2p.abc import (
    IListener,
    IMuxedConn,
    INetConn,
    INetStream,
    INetworkService,
    INotifee,
    IPeerStore,
    ITransport,
)
from libp2p.custom_types import (
    StreamHandlerFn,
)
from libp2p.io.abc import (
    ReadWriteCloser,
)
from libp2p.network.config import ConnectionConfig, RetryConfig
from libp2p.network.connection_gate import ConnectionGate
from libp2p.network.connection_pruner import ConnectionPruner
from libp2p.network.dial_queue import DialQueue
from libp2p.network.rate_limiter import ConnectionRateLimiter
from libp2p.network.reconnect_queue import ReconnectQueue
from libp2p.peer.id import (
    ID,
)
from libp2p.peer.peerstore import (
    PeerStoreError,
)
from libp2p.relay.circuit_v2.nat import extract_ip_from_multiaddr
from libp2p.tools.async_service import (
    Service,
)
from libp2p.transport.exceptions import (
    MuxerUpgradeFailure,
    OpenConnectionError,
    SecurityUpgradeFailure,
)
from libp2p.transport.quic.config import QUICTransportConfig
from libp2p.transport.quic.connection import QUICConnection
from libp2p.transport.quic.transport import QUICTransport
from libp2p.transport.upgrader import (
    TransportUpgrader,
)

from ..exceptions import (
    MultiError,
)
from .connection.raw_connection import (
    RawConnection,
)
from .connection.swarm_connection import (
    SwarmConn,
)
from .exceptions import (
    SwarmException,
)

logger = logging.getLogger("libp2p.network.swarm")


def create_default_stream_handler(network: INetworkService) -> StreamHandlerFn:
    async def stream_handler(stream: INetStream) -> None:
        await network.get_manager().wait_finished()

    return stream_handler


class Swarm(Service, INetworkService):
    self_id: ID
    peerstore: IPeerStore
    upgrader: TransportUpgrader
    transport: ITransport
    connections: dict[ID, list[INetConn]]
    listeners: dict[str, IListener]
    common_stream_handler: StreamHandlerFn
    listener_nursery: trio.Nursery | None
    event_listener_nursery_created: trio.Event

    notifees: list[INotifee]

    # Enhanced: New configuration
    retry_config: RetryConfig
    connection_config: ConnectionConfig | QUICTransportConfig
    _round_robin_index: dict[ID, int]

    # Connection queues
    dial_queue: DialQueue | None
    reconnect_queue: ReconnectQueue | None
    connection_pruner: ConnectionPruner | None
    rate_limiter: ConnectionRateLimiter | None
    connection_gate: ConnectionGate | None

    def __init__(
        self,
        peer_id: ID,
        peerstore: IPeerStore,
        upgrader: TransportUpgrader,
        transport: ITransport,
        retry_config: RetryConfig | None = None,
        connection_config: ConnectionConfig | QUICTransportConfig | None = None,
    ):
        self.self_id = peer_id
        self.peerstore = peerstore
        self.upgrader = upgrader
        self.transport = transport

        # Enhanced: Initialize retry and connection configuration
        self.retry_config = retry_config or RetryConfig()
        self.connection_config = connection_config or ConnectionConfig()

        # Enhanced: Initialize connections as 1:many mapping
        self.connections = {}
        self.listeners = dict()

        # Create Notifee array
        self.notifees = []

        self.common_stream_handler = create_default_stream_handler(self)

        self.listener_nursery = None
        self.event_listener_nursery_created = trio.Event()

        # Load balancing state
        self._round_robin_index = {}

        # Connection limit tracking
        self._incoming_pending_connections = 0
        self._outbound_pending_connections = 0

        # Initialize connection queues
        self.dial_queue = DialQueue(
            swarm=self,
            max_parallel_dials=self.connection_config.max_parallel_dials,
            max_dial_queue_length=self.connection_config.max_dial_queue_length,
            dial_timeout=self.connection_config.dial_timeout,
        )

        self.reconnect_queue = ReconnectQueue(
            swarm=self,
            retries=self.connection_config.reconnect_retries,
            retry_interval=self.connection_config.reconnect_retry_interval,
            backoff_factor=self.connection_config.reconnect_backoff_factor,
            max_parallel_reconnects=self.connection_config.max_parallel_reconnects,
        )

        self.connection_pruner = ConnectionPruner(
            swarm=self,
            allow_list=self.connection_config.allow_list or [],
        )

        # Initialize rate limiter for incoming connections
        self.rate_limiter = ConnectionRateLimiter(
            points=self.connection_config.inbound_connection_threshold,
            duration=1.0,  # Per second
            block_duration=0.0,  # No blocking by default
        )

        # Initialize connection gate for allow/deny lists
        self.connection_gate = ConnectionGate(
            allow_list=self.connection_config.allow_list,
            deny_list=self.connection_config.deny_list,
            allow_private_addresses=True,  # Default to allowing private addresses
        )

    async def run(self) -> None:
        async with trio.open_nursery() as nursery:
            # Create a nursery for listener tasks.
            self.listener_nursery = nursery
            self.event_listener_nursery_created.set()

            if isinstance(self.transport, QUICTransport):
                self.transport.set_background_nursery(nursery)
                self.transport.set_swarm(self)

            # Start connection queues and pruner
            if self.dial_queue:
                await self.dial_queue.start()
            if self.reconnect_queue:
                await self.reconnect_queue.start()
            if self.connection_pruner:
                await self.connection_pruner.start()

            try:
                await self.manager.wait_finished()
            finally:
                # Stop connection queues and pruner
                if self.connection_pruner:
                    await self.connection_pruner.stop()
                if self.reconnect_queue:
                    await self.reconnect_queue.stop()
                if self.dial_queue:
                    await self.dial_queue.stop()

                # The service ended. Cancel listener tasks.
                nursery.cancel_scope.cancel()
                # Indicate that the nursery has been cancelled.
                self.listener_nursery = None

    def get_peer_id(self) -> ID:
        return self.self_id

    def set_stream_handler(self, stream_handler: StreamHandlerFn) -> None:
        self.common_stream_handler = stream_handler

    def get_connections(self, peer_id: ID | None = None) -> list[INetConn]:
        """
        Get connections for peer (like JS getConnections, Go ConnsToPeer).

        Parameters
        ----------
        peer_id : ID | None
            The peer ID to get connections for. If None, returns all connections.

        Returns
        -------
        list[INetConn]
            List of connections to the specified peer, or all connections
            if peer_id is None.

        """
        if peer_id is not None:
            return self.connections.get(peer_id, [])

        # Return all connections from all peers
        all_conns = []
        for conns in self.connections.values():
            all_conns.extend(conns)
        return all_conns

    def get_connections_map(self) -> dict[ID, list[INetConn]]:
        """
        Get all connections map (like JS getConnectionsMap).

        Returns
        -------
        dict[ID, list[INetConn]]
            The complete mapping of peer IDs to their connection lists.

        """
        return self.connections.copy()

    def get_connection(self, peer_id: ID) -> INetConn | None:
        """
        Get single connection for backward compatibility.

        Parameters
        ----------
        peer_id : ID
            The peer ID to get a connection for.

        Returns
        -------
        INetConn | None
            The first available connection, or None if no connections exist.

        """
        conns = self.get_connections(peer_id)
        return conns[0] if conns else None

    def get_total_connections(self) -> int:
        """
        Get total number of active connections.

        Returns
        -------
        int
            Total number of connections across all peers

        """
        return sum(len(conns) for conns in self.connections.values())

    def accept_incoming_connection(self, remote_addr: Multiaddr | None = None) -> bool:
        """
        Check if an incoming connection should be accepted.

        Checks:
        - Deny list (if configured)
        - Allow list (always accepts if in allow list)
        - Pending connections limit
        - Global connection limit

        Parameters
        ----------
        remote_addr : Multiaddr | None
            Remote address of the incoming connection (for allow/deny list checks)

        Returns
        -------
        bool
            True if connection should be accepted, False otherwise

        """
        # Check connection gate (allow/deny lists)
        if remote_addr and self.connection_gate:
            # Check deny list first (deny takes precedence)
            if not self.connection_gate.is_allowed(remote_addr):
                logger.debug(
                    f"Connection from {remote_addr} refused - "
                    "connection remote address was in deny list or not in allow list"
                )
                return False

            # Check if connection is in allow list (if allow list exists)
            # If in allow list, skip rate limiting and go straight to limits check
            if (
                self.connection_config.allow_list
                and self.connection_gate.is_in_allow_list(remote_addr)
            ):
                # Connection is in allow list - skip rate limiting
                # Still check pending/global limits
                max_pending = self.connection_config.max_incoming_pending_connections
                if self._incoming_pending_connections >= max_pending:
                    logger.debug(
                        f"Connection from {remote_addr} refused - "
                        f"incomingPendingConnections exceeded"
                    )
                    return False

                total_connections = self.get_total_connections()
                if total_connections >= self.connection_config.max_connections:
                    logger.debug(
                        f"Connection from {remote_addr} refused - "
                        f"maxConnections exceeded"
                    )
                    return False

                # Accept connection from allow list
                self._incoming_pending_connections += 1
                return True

        # Check pending connections limit
        max_pending = self.connection_config.max_incoming_pending_connections
        if self._incoming_pending_connections >= max_pending:
            logger.debug(
                f"Connection from {remote_addr} refused - "
                f"incomingPendingConnections ({self._incoming_pending_connections}) "
                f"exceeded limit ({max_pending})"
            )
            return False

        # Check rate limiting (per-host) - only if not in allow list
        if remote_addr and self.rate_limiter:
            host = extract_ip_from_multiaddr(remote_addr)
            if host:
                if not self.rate_limiter.check_and_consume(host):
                    logger.debug(
                        f"Connection from {remote_addr} refused - "
                        f"inboundConnectionThreshold exceeded by host {host}"
                    )
                    return False

        # Check global connection limit
        total_connections = self.get_total_connections()
        if total_connections >= self.connection_config.max_connections:
            max_conns = self.connection_config.max_connections
            logger.debug(
                f"Connection from {remote_addr} refused - "
                f"maxConnections ({total_connections}/{max_conns}) exceeded"
            )
            return False

        # Accept connection - increment pending counter
        self._incoming_pending_connections += 1
        return True

    def after_upgrade_inbound(self) -> None:
        """
        Called after an inbound connection is upgraded.

        Decrements the incoming pending connections counter.
        """
        if self._incoming_pending_connections > 0:
            self._incoming_pending_connections -= 1

    async def dial_peer(self, peer_id: ID, priority: int = 50) -> list[INetConn]:
        """
        Try to create connections to peer_id using dial queue or direct dial.

        :param peer_id: peer if we want to dial
        :param priority: dial priority (higher = processed first, default: 50)
        :raises SwarmException: raised when an error occurs
        :return: list of muxed connections
        """
        # Check if we already have connections
        existing_connections = self.get_connections(peer_id)
        if existing_connections:
            logger.debug(f"Reusing existing connections to peer {peer_id}")
            return existing_connections

        # Use dial queue if available
        if (
            self.dial_queue
            and hasattr(self.dial_queue, "_started")
            and self.dial_queue._started
        ):
            try:
                logger.debug("attempting to dial peer %s via dial queue", peer_id)
                connection = await self.dial_queue.dial(peer_id, priority=priority)
                return [connection]
            except Exception as e:
                logger.debug(
                    f"Dial queue failed for {peer_id}, "
                    f"falling back to direct dial: {e}"
                )

        # Fall back to direct dial (original logic)
        logger.debug("attempting to dial peer %s (direct)", peer_id)

        try:
            # Get peer info from peer store
            addrs = self.peerstore.addrs(peer_id)
        except PeerStoreError as error:
            raise SwarmException(f"No known addresses to peer {peer_id}") from error

        if not addrs:
            raise SwarmException(f"No known addresses to peer {peer_id}")

        connections = []
        exceptions: list[SwarmException] = []

        # Enhanced: Try all known addresses with retry logic
        for multiaddr in addrs:
            try:
                connection = await self._dial_with_retry(multiaddr, peer_id)
                connections.append(connection)

                # Limit number of connections per peer
                if len(connections) >= self.connection_config.max_connections_per_peer:
                    break

            except SwarmException as e:
                exceptions.append(e)
                logger.debug(
                    "encountered swarm exception when trying to connect to %s, "
                    "trying next address...",
                    multiaddr,
                    exc_info=e,
                )

        if not connections:
            # Tried all addresses, raising exception.
            raise SwarmException(
                f"unable to connect to {peer_id}, no addresses established a "
                "successful connection (with exceptions)"
            ) from MultiError(exceptions)

        return connections

    async def _dial_with_retry(self, addr: Multiaddr, peer_id: ID) -> INetConn:
        """
        Enhanced: Dial with retry logic and exponential backoff.

        :param addr: the address to dial
        :param peer_id: the peer we want to connect to
        :raises SwarmException: raised when all retry attempts fail
        :return: network connection
        """
        last_exception = None

        for attempt in range(self.retry_config.max_retries + 1):
            try:
                return await self._dial_addr_single_attempt(addr, peer_id)
            except Exception as e:
                last_exception = e
                if attempt < self.retry_config.max_retries:
                    delay = self._calculate_backoff_delay(attempt)
                    logger.debug(
                        f"Connection attempt {attempt + 1} failed, "
                        f"retrying in {delay:.2f}s: {e}"
                    )
                    await trio.sleep(delay)
                else:
                    logger.debug(f"All {self.retry_config.max_retries} attempts failed")

        # Convert the last exception to SwarmException for consistency
        if last_exception is not None:
            if isinstance(last_exception, SwarmException):
                raise last_exception
            else:
                raise SwarmException(
                    f"Failed to connect after {self.retry_config.max_retries} attempts"
                ) from last_exception

        # This should never be reached, but mypy requires it
        raise SwarmException("Unexpected error in retry logic")

    def _calculate_backoff_delay(self, attempt: int) -> float:
        """
        Enhanced: Calculate backoff delay with jitter to prevent thundering herd.

        :param attempt: the current attempt number (0-based)
        :return: delay in seconds
        """
        delay = min(
            self.retry_config.initial_delay
            * (self.retry_config.backoff_multiplier**attempt),
            self.retry_config.max_delay,
        )

        # Add jitter to prevent synchronized retries
        jitter = delay * self.retry_config.jitter_factor
        return delay + random.uniform(-jitter, jitter)

    async def _dial_addr_single_attempt(self, addr: Multiaddr, peer_id: ID) -> INetConn:
        """
        Enhanced: Single attempt to dial an address (extracted from original dial_addr).

        :param addr: the address we want to connect with
        :param peer_id: the peer we want to connect to
        :raises SwarmException: raised when an error occurs
        :return: network connection
        """
        # Dial peer (connection to peer does not yet exist)
        # Transport dials peer (gets back a raw conn)
        try:
            addr = Multiaddr(f"{addr}/p2p/{peer_id}")
            raw_conn = await self.transport.dial(addr)
        except OpenConnectionError as error:
            logger.debug("fail to dial peer %s over base transport", peer_id)
            raise SwarmException(
                f"fail to open connection to peer {peer_id}"
            ) from error

        if isinstance(self.transport, QUICTransport) and isinstance(
            raw_conn, IMuxedConn
        ):
            logger.info(
                "Skipping upgrade for QUIC, QUIC connections are already multiplexed"
            )
            swarm_conn = await self.add_conn(raw_conn)
            return swarm_conn

        logger.debug("dialed peer %s over base transport", peer_id)

        # Per, https://discuss.libp2p.io/t/multistream-security/130, we first secure
        # the conn and then mux the conn
        try:
            secured_conn = await self.upgrader.upgrade_security(raw_conn, True, peer_id)
        except SecurityUpgradeFailure as error:
            logger.debug("failed to upgrade security for peer %s", peer_id)
            await raw_conn.close()
            raise SwarmException(
                f"failed to upgrade security for peer {peer_id}"
            ) from error

        logger.debug("upgraded security for peer %s", peer_id)

        try:
            muxed_conn = await self.upgrader.upgrade_connection(secured_conn, peer_id)
        except MuxerUpgradeFailure as error:
            logger.debug("failed to upgrade mux for peer %s", peer_id)
            await secured_conn.close()
            raise SwarmException(f"failed to upgrade mux for peer {peer_id}") from error

        logger.debug("upgraded mux for peer %s", peer_id)

        swarm_conn = await self.add_conn(muxed_conn)
        logger.debug("successfully dialed peer %s", peer_id)
        return swarm_conn

    async def dial_addr(self, addr: Multiaddr, peer_id: ID) -> INetConn:
        """
        Enhanced: Try to create a connection to peer_id with addr using retry logic.

        :param addr: the address we want to connect with
        :param peer_id: the peer we want to connect to
        :raises SwarmException: raised when an error occurs
        :return: network connection
        """
        return await self._dial_with_retry(addr, peer_id)

    async def new_stream(self, peer_id: ID) -> INetStream:
        """
        Enhanced: Create a new stream with load balancing across multiple connections.

        :param peer_id: peer_id of destination
        :raises SwarmException: raised when an error occurs
        :return: net stream instance
        """
        logger.debug("attempting to open a stream to peer %s", peer_id)
        # Get existing connections or dial new ones
        connections = self.get_connections(peer_id)
        if not connections:
            connections = await self.dial_peer(peer_id)

        # Load balancing strategy at interface level
        connection = self._select_connection(connections, peer_id)

        if isinstance(self.transport, QUICTransport) and connection is not None:
            conn = cast(SwarmConn, connection)
            return await conn.new_stream()

        try:
            net_stream = await connection.new_stream()
            logger.debug("successfully opened a stream to peer %s", peer_id)
            return net_stream
        except Exception as e:
            logger.debug(f"Failed to create stream on connection: {e}")
            # Try other connections if available
            for other_conn in connections:
                if other_conn != connection:
                    try:
                        net_stream = await other_conn.new_stream()
                        logger.debug(
                            f"Successfully opened a stream to peer {peer_id} "
                            "using alternative connection"
                        )
                        return net_stream
                    except Exception:
                        continue

            # All connections failed, raise exception
            raise SwarmException(f"Failed to create stream to peer {peer_id}") from e

    def _select_connection(self, connections: list[INetConn], peer_id: ID) -> INetConn:
        """
        Select connection based on load balancing strategy.

        Parameters
        ----------
        connections : list[INetConn]
            List of available connections.
        peer_id : ID
            The peer ID for round-robin tracking.
        strategy : str
            Load balancing strategy ("round_robin", "least_loaded", etc.).

        Returns
        -------
        INetConn
            Selected connection.

        """
        if not connections:
            raise ValueError("No connections available")

        strategy = self.connection_config.load_balancing_strategy

        if strategy == "round_robin":
            # Simple round-robin selection
            if peer_id not in self._round_robin_index:
                self._round_robin_index[peer_id] = 0

            index = self._round_robin_index[peer_id] % len(connections)
            self._round_robin_index[peer_id] += 1
            return connections[index]

        elif strategy == "least_loaded":
            # Find connection with least streams
            return min(connections, key=lambda c: len(c.get_streams()))

        else:
            # Default to first connection
            return connections[0]

    async def listen(self, *multiaddrs: Multiaddr) -> bool:
        """
        :param multiaddrs: one or many multiaddrs to start listening on
        :return: true if at least one success

        For each multiaddr

          - Check if a listener for multiaddr exists already
          - If listener already exists, continue
          - Otherwise:

              - Capture multiaddr in conn handler
              - Have conn handler delegate to stream handler
              - Call listener listen with the multiaddr
              - Map multiaddr to listener
        """
        logger.debug(f"Swarm.listen called with multiaddrs: {multiaddrs}")
        # We need to wait until `self.listener_nursery` is created.
        logger.debug("Starting to listen")
        await self.event_listener_nursery_created.wait()

        success_count = 0
        for maddr in multiaddrs:
            logger.debug(f"Swarm.listen processing multiaddr: {maddr}")
            if str(maddr) in self.listeners:
                logger.debug(f"Swarm.listen: listener already exists for {maddr}")
                success_count += 1
                continue

            async def conn_handler(
                read_write_closer: ReadWriteCloser, maddr: Multiaddr = maddr
            ) -> None:
                # Extract remote address if available
                remote_addr = None
                if hasattr(read_write_closer, "remote_addr") or hasattr(
                    read_write_closer, "_remote_addr"
                ):
                    remote_addr = getattr(
                        read_write_closer, "remote_addr", None
                    ) or getattr(read_write_closer, "_remote_addr", None)

                # Check connection limits before accepting
                if not self.accept_incoming_connection(remote_addr or maddr):
                    logger.debug(
                        f"Rejecting incoming connection from "
                        f"{remote_addr or maddr or 'unknown'}"
                    )
                    await read_write_closer.close()
                    return

                # No need to upgrade QUIC Connection
                if isinstance(self.transport, QUICTransport):
                    try:
                        quic_conn = cast(QUICConnection, read_write_closer)
                        # Mark as accepted (after upgrade)
                        self.after_upgrade_inbound()
                        await self.add_conn(quic_conn)
                        peer_id = quic_conn.peer_id
                        logger.debug(
                            f"successfully opened quic connection to peer {peer_id}"
                        )
                        # NOTE: This is a intentional barrier to prevent from the
                        # handler exiting and closing the connection.
                        await self.manager.wait_finished()
                    except Exception:
                        await read_write_closer.close()
                        # Decrement pending counter on error
                        self.after_upgrade_inbound()
                    return

                raw_conn = RawConnection(read_write_closer, False)

                # Per, https://discuss.libp2p.io/t/multistream-security/130, we first
                # secure the conn and then mux the conn
                try:
                    secured_conn = await self.upgrader.upgrade_security(raw_conn, False)
                except SecurityUpgradeFailure as error:
                    logger.debug("failed to upgrade security for peer at %s", maddr)
                    await raw_conn.close()
                    # Decrement pending counter on error
                    self.after_upgrade_inbound()
                    raise SwarmException(
                        f"failed to upgrade security for peer at {maddr}"
                    ) from error

                # Mark as accepted (after security upgrade)
                self.after_upgrade_inbound()
                peer_id = secured_conn.get_remote_peer()

                try:
                    muxed_conn = await self.upgrader.upgrade_connection(
                        secured_conn, peer_id
                    )
                except MuxerUpgradeFailure as error:
                    logger.debug("fail to upgrade mux for peer %s", peer_id)
                    await secured_conn.close()
                    raise SwarmException(
                        f"fail to upgrade mux for peer {peer_id}"
                    ) from error
                logger.debug("upgraded mux for peer %s", peer_id)

                await self.add_conn(muxed_conn)
                logger.debug("successfully opened connection to peer %s", peer_id)

                # NOTE: This is a intentional barrier to prevent from the handler
                # exiting and closing the connection.
                await self.manager.wait_finished()

            try:
                # Success
                logger.debug(f"Swarm.listen: creating listener for {maddr}")
                listener = self.transport.create_listener(conn_handler)
                logger.debug(f"Swarm.listen: listener created for {maddr}")
                self.listeners[str(maddr)] = listener
                # TODO: `listener.listen` is not bounded with nursery. If we want to be
                #   I/O agnostic, we should change the API.
                if self.listener_nursery is None:
                    raise SwarmException("swarm instance hasn't been run")
                assert self.listener_nursery is not None  # For type checker
                logger.debug(f"Swarm.listen: calling listener.listen for {maddr}")
                await listener.listen(maddr, self.listener_nursery)
                logger.debug(f"Swarm.listen: listener.listen completed for {maddr}")

                # Call notifiers since event occurred
                await self.notify_listen(maddr)

                success_count += 1
                logger.debug("successfully started listening on: %s", maddr)
            except OSError:
                # Failed. Continue looping.
                logger.debug("fail to listen on: %s", maddr)

        # Return true if at least one address succeeded
        return success_count > 0

    async def close(self) -> None:
        """
        Close the swarm instance and cleanup resources.
        """
        # Check if manager exists before trying to stop it
        if hasattr(self, "_manager") and self._manager is not None:
            await self._manager.stop()
        else:
            # Perform alternative cleanup if the manager isn't initialized
            # Stop connection queues and pruner first
            if hasattr(self, "connection_pruner") and self.connection_pruner:
                await self.connection_pruner.stop()
            if hasattr(self, "reconnect_queue") and self.reconnect_queue:
                await self.reconnect_queue.stop()
            if hasattr(self, "dial_queue") and self.dial_queue:
                await self.dial_queue.stop()

            # Close all connections manually
            if hasattr(self, "connections"):
                for peer_id, conns in list(self.connections.items()):
                    for conn in conns:
                        await conn.close()

                # Clear connection tracking dictionary
                self.connections.clear()

            # Close all listeners
            if hasattr(self, "listeners"):
                for maddr_str, listener in self.listeners.items():
                    await listener.close()
                    # Notify about listener closure
                    try:
                        multiaddr = Multiaddr(maddr_str)
                        await self.notify_listen_close(multiaddr)
                    except Exception as e:
                        logger.warning(
                            f"Failed to notify listen_close for {maddr_str}: {e}"
                        )
                self.listeners.clear()

            # Close the transport if it exists and has a close method
            if hasattr(self, "transport") and self.transport is not None:
                # Check if transport has close method before calling it
                if hasattr(self.transport, "close"):
                    await self.transport.close()  # type: ignore
                # Ignoring the type above since `transport` may not have a close method
                # and we have already checked it with hasattr

        logger.debug("swarm successfully closed")

    async def close_peer(self, peer_id: ID) -> None:
        """
        Close all connections to the specified peer.

        Parameters
        ----------
        peer_id : ID
            The peer ID to close connections for.

        """
        connections = self.get_connections(peer_id)
        if not connections:
            return

        # Close all connections
        for connection in connections:
            try:
                await connection.close()
            except Exception as e:
                logger.warning(f"Error closing connection to {peer_id}: {e}")

        # Remove from connections dict
        self.connections.pop(peer_id, None)

        logger.debug("successfully close the connection to peer %s", peer_id)

    async def add_conn(self, muxed_conn: IMuxedConn) -> SwarmConn:
        """
        Add a `IMuxedConn` to `Swarm` as a `SwarmConn`, notify "connected",
        and start to monitor the connection for its new streams and
        disconnection.
        """
        swarm_conn = SwarmConn(
            muxed_conn,
            self,
        )
        logger.debug("Swarm::add_conn | starting muxed connection")
        self.manager.run_task(muxed_conn.start)
        await muxed_conn.event_started.wait()
        logger.debug("Swarm::add_conn | starting swarm connection")
        self.manager.run_task(swarm_conn.start)
        await swarm_conn.event_started.wait()

        # Add to connections dict with deduplication
        peer_id = muxed_conn.peer_id
        if peer_id not in self.connections:
            self.connections[peer_id] = []

        # Check for duplicate connections by comparing the underlying muxed connection
        for existing_conn in self.connections[peer_id]:
            if existing_conn.muxed_conn == muxed_conn:
                logger.debug(f"Connection already exists for peer {peer_id}")
                # existing_conn is a SwarmConn since it's stored in the connections list
                return existing_conn  # type: ignore[return-value]

        self.connections[peer_id].append(swarm_conn)

        # Trim if we exceed max connections per peer
        max_conns = self.connection_config.max_connections_per_peer
        if len(self.connections[peer_id]) > max_conns:
            self._trim_connections(peer_id)

        # Trigger connection pruner to check global limits
        if self.connection_pruner:
            # Schedule pruning check (non-blocking)
            try:
                if hasattr(self, "manager") and self.manager is not None:
                    self.manager.run_task(self.connection_pruner.maybe_prune_connections)
            except AttributeError:
                # Fallback if manager not available
                trio.lowlevel.spawn_system_task(
                    self.connection_pruner.maybe_prune_connections
                )

        # Call notifiers since event occurred
        await self.notify_connected(swarm_conn)
        return swarm_conn

    def _trim_connections(self, peer_id: ID) -> None:
        """
        Remove oldest connections when limit is exceeded.
        """
        connections = self.connections[peer_id]
        if len(connections) <= self.connection_config.max_connections_per_peer:
            return

        # Sort by creation time and remove oldest
        # For now, just keep the most recent connections
        max_conns = self.connection_config.max_connections_per_peer
        connections_to_remove = connections[:-max_conns]

        for conn in connections_to_remove:
            logger.debug(f"Trimming old connection for peer {peer_id}")
            trio.lowlevel.spawn_system_task(self._close_connection_async, conn)

        # Keep only the most recent connections
        max_conns = self.connection_config.max_connections_per_peer
        self.connections[peer_id] = connections[-max_conns:]

    async def _close_connection_async(self, connection: INetConn) -> None:
        """Close a connection asynchronously."""
        try:
            await connection.close()
        except Exception as e:
            logger.warning(f"Error closing connection: {e}")

    def remove_conn(self, swarm_conn: SwarmConn) -> None:
        """
        Simply remove the connection from Swarm's records, without closing
        the connection.

        If this was the last connection to the peer, triggers reconnection
        queue check for KEEP_ALIVE peers.
        """
        peer_id = swarm_conn.muxed_conn.peer_id

        if peer_id in self.connections:
            self.connections[peer_id] = [
                conn for conn in self.connections[peer_id] if conn != swarm_conn
            ]
            if not self.connections[peer_id]:
                # Last connection to peer removed - trigger reconnection check
                del self.connections[peer_id]

                # Check if peer should be reconnected (KEEP_ALIVE)
                if self.reconnect_queue:
                    try:
                        if hasattr(self, "manager") and self.manager is not None:
                            self.manager.run_task(
                                self.reconnect_queue.maybe_reconnect, peer_id
                            )
                        else:
                            # Manager not available - spawn using trio directly
                            trio.lowlevel.spawn_system_task(
                                self.reconnect_queue.maybe_reconnect, peer_id
                            )
                    except AttributeError:
                        # Fallback if manager not available
                        trio.lowlevel.spawn_system_task(
                            self.reconnect_queue.maybe_reconnect, peer_id
                        )

    # Notifee

    def register_notifee(self, notifee: INotifee) -> None:
        """
        :param notifee: object implementing Notifee interface
        :return: true if notifee registered successfully, false otherwise
        """
        self.notifees.append(notifee)

    async def notify_opened_stream(self, stream: INetStream) -> None:
        async with trio.open_nursery() as nursery:
            for notifee in self.notifees:
                nursery.start_soon(notifee.opened_stream, self, stream)

    async def notify_connected(self, conn: INetConn) -> None:
        async with trio.open_nursery() as nursery:
            for notifee in self.notifees:
                nursery.start_soon(notifee.connected, self, conn)

    async def notify_disconnected(self, conn: INetConn) -> None:
        async with trio.open_nursery() as nursery:
            for notifee in self.notifees:
                nursery.start_soon(notifee.disconnected, self, conn)

    async def notify_listen(self, multiaddr: Multiaddr) -> None:
        async with trio.open_nursery() as nursery:
            for notifee in self.notifees:
                nursery.start_soon(notifee.listen, self, multiaddr)

    async def notify_closed_stream(self, stream: INetStream) -> None:
        async with trio.open_nursery() as nursery:
            for notifee in self.notifees:
                nursery.start_soon(notifee.closed_stream, self, stream)

    async def notify_listen_close(self, multiaddr: Multiaddr) -> None:
        async with trio.open_nursery() as nursery:
            for notifee in self.notifees:
                nursery.start_soon(notifee.listen_close, self, multiaddr)

    # Generic notifier used by NetStream._notify_closed
    async def notify_all(self, notifier: Callable[[INotifee], Awaitable[None]]) -> None:
        async with trio.open_nursery() as nursery:
            for notifee in self.notifees:
                nursery.start_soon(notifier, notifee)

    def get_dial_queue(self) -> list[dict[str, Any]]:
        """
        Get list of pending dials in the queue.

        Note: This is a synchronous method that returns a snapshot of the queue.
        For async-safe access, the actual queue state is read without locks
        (may show slightly stale data).

        Returns
        -------
        list[dict]
            List of pending dial information (matching JS libp2p PendingDial format)

        """
        if not self.dial_queue:
            return []

        # Return queue status information (snapshot without locking for sync access)
        queue_info = []

        # Read queue snapshot (may be slightly stale but safe for sync method)
        queue_snapshot = list(self.dial_queue._queue)
        for job in queue_snapshot:
            queue_info.append({
                "id": job.job_id,
                "peer_id": job.peer_id,
                "multiaddrs": [Multiaddr(ma) for ma in job.multiaddrs],
                "priority": job.priority,
                "status": "queued" if not job.running else "running",
            })

        # Also include running jobs snapshot
        running_snapshot = list(self.dial_queue._running_jobs)
        for job in running_snapshot:
            queue_info.append({
                "id": job.job_id,
                "peer_id": job.peer_id,
                "multiaddrs": [Multiaddr(ma) for ma in job.multiaddrs],
                "priority": job.priority,
                "status": "running",
            })

        return queue_info

    # Backward compatibility properties
    @property
    def connections_legacy(self) -> dict[ID, INetConn]:
        """
        Legacy 1:1 mapping for backward compatibility.

        Returns
        -------
        dict[ID, INetConn]
            Legacy mapping with only the first connection per peer.

        """
        legacy_conns = {}
        for peer_id, conns in self.connections.items():
            if conns:
                legacy_conns[peer_id] = conns[0]
        return legacy_conns
