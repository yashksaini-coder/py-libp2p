"""WebSocket listener for libp2p."""

from collections.abc import Awaitable, Callable
import logging
from typing import Any

from multiaddr import Multiaddr
import trio
from trio_typing import TaskStatus

from libp2p.abc import IListener
from libp2p.custom_types import THandler
from libp2p.transport.upgrader import TransportUpgrader

from .connection import P2PWebSocketConnection

logger = logging.getLogger("libp2p.transport.websocket.listener")

try:
    from trio_websocket import serve_websocket
except ImportError as e:
    logger.error(
        "trio_websocket is required for WebSocket transport. "
        "Install with: pip install trio-websocket"
    )
    raise ImportError("trio_websocket is required for WebSocket transport") from e


class WebsocketListener(IListener):
    """
    Listen on /ip4/.../tcp/.../ws addresses, handshake WS, wrap into RawConnection.

    Supports IPv4, IPv6, and DNS addresses with proper URL formatting for WebSocket.
    """

    def __init__(self, handler: THandler, upgrader: TransportUpgrader) -> None:
        self._handler = handler
        self._upgrader = upgrader
        self._server: Any | None = None
        self._shutdown_event = trio.Event()
        self._nursery: trio.Nursery | None = None
        self._listeners: Any | None = None

    async def listen(self, maddr: Multiaddr, nursery: trio.Nursery) -> bool:
        """Start listening on the given multiaddr."""
        logger.debug(f"WebsocketListener.listen called with {maddr}")
        addr_str = str(maddr)

        # Check for TLS WebSocket support (not yet implemented)
        if addr_str.endswith("/wss"):
            raise NotImplementedError("/wss (TLS) not yet supported")

        # Validate that this is a WebSocket multiaddr
        if not addr_str.endswith("/ws"):
            raise ValueError(
                f"WebsocketListener only supports /ws addresses, got {maddr}"
            )

        # Extract host and port from multiaddr with proper IPv6 support
        host = self._extract_host_from_multiaddr(maddr)
        port = self._extract_port_from_multiaddr(maddr)

        logger.debug(f"WebsocketListener: host={host}, port={port}")

        async def serve_websocket_tcp(
            handler: Callable[[Any], Awaitable[None]],
            port: int,
            host: str,
            task_status: TaskStatus[Any],
        ) -> None:
            """Start TCP server and handle WebSocket connections."""
            logger.debug("serve_websocket_tcp %s %s", host, port)

            async def websocket_handler(request: Any) -> None:
                """Handle WebSocket requests."""
                logger.debug("WebSocket request received")
                try:
                    # Accept the WebSocket connection
                    ws_connection = await request.accept()
                    logger.debug("WebSocket handshake successful")

                    # Create the WebSocket connection wrapper
                    conn = P2PWebSocketConnection(ws_connection)

                    # Call the handler function that was passed to create_listener
                    # This handler will handle the security and muxing upgrades
                    logger.debug("Calling connection handler")
                    await self._handler(conn)

                    logger.debug("Handler completed successfully")

                except Exception as e:
                    logger.debug(f"WebSocket connection error: {e}")
                    logger.debug(f"Error type: {type(e)}")
                    import traceback

                    logger.debug(f"Traceback: {traceback.format_exc()}")
                    # Reject the connection
                    try:
                        await request.reject(400)
                    except Exception:
                        pass

            # Use trio_websocket.serve_websocket for proper WebSocket handling
            await serve_websocket(
                websocket_handler, host, port, None, task_status=task_status
            )

        # Store the nursery for shutdown
        self._nursery = nursery

        # Start the server using nursery.start() like TCP does
        logger.debug("Calling nursery.start()...")
        try:
            started_listeners = await nursery.start(
                serve_websocket_tcp,
                None,  # No handler needed since it's defined inside serve_websocket_tcp
                port,
                host,
            )
            logger.debug(f"nursery.start() returned: {started_listeners}")

            if started_listeners is None:
                logger.error(f"Failed to start WebSocket listener for {maddr}")
                return False

            # Store the listeners for get_addrs() and close()
            self._listeners = started_listeners
            logger.debug(
                "WebsocketListener.listen returning True with WebSocketServer object"
            )
            return True

        except Exception as e:
            logger.error(f"Failed to start WebSocket listener for {maddr}: {e}")
            return False

    def _extract_host_from_multiaddr(self, maddr: Multiaddr) -> str:
        """Extract host from multiaddr with proper IPv6 support."""
        # Try different protocol types in order of preference
        host = (
            maddr.value_for_protocol("ip4")
            or maddr.value_for_protocol("ip6")
            or maddr.value_for_protocol("dns4")
            or maddr.value_for_protocol("dns6")
            or maddr.value_for_protocol("dns")
            or maddr.value_for_protocol("dnsaddr")
        )

        if host is None:
            # Default to listening on all interfaces
            return "0.0.0.0"

        return host

    def _extract_port_from_multiaddr(self, maddr: Multiaddr) -> int:
        """Extract port from multiaddr."""
        port_str = maddr.value_for_protocol("tcp")
        if port_str is None:
            raise ValueError(f"No TCP port found in multiaddr: {maddr}")

        try:
            return int(port_str)
        except ValueError:
            raise ValueError(f"Invalid TCP port '{port_str}' in multiaddr: {maddr}")

    def get_addrs(self) -> tuple[Multiaddr, ...]:
        """Get the addresses this listener is listening on."""
        if not hasattr(self, "_listeners") or not self._listeners:
            logger.debug("No listeners available for get_addrs()")
            return ()

        try:
            # Handle WebSocketServer objects
            if hasattr(self._listeners, "port"):
                # This is a WebSocketServer object
                port = self._listeners.port

                # Try to determine the actual host we're listening on
                host = "127.0.0.1"  # Default
                if hasattr(self._listeners, "socket"):
                    try:
                        sockname = self._listeners.socket.getsockname()
                        if sockname and len(sockname) >= 2:
                            host = str(sockname[0])
                            # Use 127.0.0.1 instead of 0.0.0.0 for client connections
                            if host == "0.0.0.0":
                                host = "127.0.0.1"
                    except Exception as e:
                        logger.debug(f"Could not get socket name: {e}")

                # Create a multiaddr from the port and host
                return (Multiaddr(f"/ip4/{host}/tcp/{port}/ws"),)

            elif isinstance(self._listeners, (list, tuple)):
                # This is a list of listeners (like TCP)
                return tuple(
                    self._multiaddr_from_socket(listener.socket)
                    for listener in self._listeners
                )
            else:
                logger.warning(f"Unknown listener type: {type(self._listeners)}")
                return ()

        except Exception as e:
            logger.warning(f"Error getting addresses: {e}")
            return ()

    def _multiaddr_from_socket(self, sock: Any) -> Multiaddr:
        """Convert socket to multiaddr."""
        try:
            sockname = sock.getsockname()
            if len(sockname) >= 2:
                host, port = sockname[0], sockname[1]

                # Handle IPv6 addresses
                if ":" in host and not host.startswith("["):
                    # This is likely IPv6
                    return Multiaddr(f"/ip6/{host}/tcp/{port}/ws")
                else:
                    # IPv4 or already formatted IPv6
                    return Multiaddr(f"/ip4/{host}/tcp/{port}/ws")
            else:
                # Fallback
                return Multiaddr("/ip4/127.0.0.1/tcp/8080/ws")
        except Exception as e:
            logger.warning(f"Error converting socket to multiaddr: {e}")
            return Multiaddr("/ip4/127.0.0.1/tcp/8080/ws")

    async def close(self) -> None:
        """Close the WebSocket listener and stop accepting new connections."""
        logger.debug("WebsocketListener.close called")
        if hasattr(self, "_listeners") and self._listeners:
            # Signal shutdown
            self._shutdown_event.set()

            # Close the WebSocket server
            try:
                if hasattr(self._listeners, "aclose"):
                    # This is a WebSocketServer object
                    logger.debug("Closing WebSocket server")
                    await self._listeners.aclose()
                    logger.debug("WebSocket server closed")
                elif isinstance(self._listeners, (list, tuple)):
                    # This is a list of listeners (like TCP)
                    logger.debug("Closing TCP listeners")
                    for listener in self._listeners:
                        if hasattr(listener, "aclose"):
                            await listener.aclose()
                    logger.debug("TCP listeners closed")
                else:
                    # Unknown type, try to close it directly
                    logger.debug("Closing unknown listener type")
                    if hasattr(self._listeners, "close"):
                        self._listeners.close()
                    logger.debug("Unknown listener closed")
            except Exception as e:
                logger.warning(f"Error closing listener: {e}")

            # Clear the listeners reference
            self._listeners = None
            logger.debug("WebsocketListener.close completed")
