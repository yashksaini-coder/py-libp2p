"""WebSocket transport for libp2p."""

import logging
import re

from multiaddr import Multiaddr

from libp2p.abc import IListener, ITransport
from libp2p.custom_types import THandler
from libp2p.network.connection.raw_connection import RawConnection
from libp2p.transport.exceptions import OpenConnectionError
from libp2p.transport.upgrader import TransportUpgrader

from .connection import P2PWebSocketConnection
from .listener import WebsocketListener

logger = logging.getLogger(__name__)

try:
    from trio_websocket import open_websocket_url
except ImportError as e:
    logger.error(
        "trio_websocket is required for WebSocket transport. "
        "Install with: pip install trio-websocket"
    )
    raise ImportError("trio_websocket is required for WebSocket transport") from e


class WebsocketTransport(ITransport):
    """
    Libp2p WebSocket transport: dial and listen on WebSocket addresses.

    Supports:
    - IPv4: /ip4/127.0.0.1/tcp/8080/ws
    - IPv6: /ip6/::1/tcp/8080/ws
    - DNS: /dns4/example.com/tcp/443/ws
    - Security protocols via TransportUpgrader (Noise, Plaintext)
    """

    def __init__(self, upgrader: TransportUpgrader) -> None:
        self._upgrader = upgrader

    async def dial(self, maddr: Multiaddr) -> RawConnection:
        """Dial a WebSocket connection to the given multiaddr."""
        logger.debug(f"WebsocketTransport.dial called with {maddr}")

        # Validate multiaddr format
        addr_str = str(maddr)
        if addr_str.endswith("/wss"):
            raise NotImplementedError("/wss (TLS) not yet supported")
        if not addr_str.endswith("/ws"):
            raise ValueError(
                f"WebsocketTransport only supports /ws addresses, got {maddr}"
            )

        # Extract host and port from multiaddr
        host = self._extract_host_from_multiaddr(maddr)
        port = self._extract_port_from_multiaddr(maddr)

        # Build WebSocket URL with proper IPv6 formatting
        ws_url = self._build_websocket_url(host, port)
        logger.debug(f"WebsocketTransport.dial connecting to {ws_url}")

        try:
            # Use the context manager but don't exit it immediately
            # The connection will be closed when the RawConnection is closed
            ws_context = open_websocket_url(ws_url)
            ws = await ws_context.__aenter__()
            conn = P2PWebSocketConnection(ws, ws_context)
            return RawConnection(conn, initiator=True)
        except Exception as e:
            logger.error(f"Failed to dial WebSocket {maddr}: {e}")
            raise OpenConnectionError(f"Failed to dial WebSocket {maddr}: {e}") from e

    def _extract_host_from_multiaddr(self, maddr: Multiaddr) -> str:
        """Extract host from multiaddr with support for various protocol types."""
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
            raise ValueError(f"No valid host protocol found in multiaddr: {maddr}")

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

    def _build_websocket_url(self, host: str, port: int) -> str:
        """
        Build WebSocket URL with proper IPv6 address formatting.

        IPv6 addresses must be wrapped in brackets in URLs.
        """
        # Check if this is an IPv6 address (contains colons but not already in brackets)
        if self._is_ipv6_address(host) and not (
            host.startswith("[") and host.endswith("]")
        ):
            # Wrap IPv6 address in brackets for URL
            formatted_host = f"[{host}]"
        else:
            formatted_host = host

        return f"ws://{formatted_host}:{port}/"

    def _is_ipv6_address(self, host: str) -> bool:
        """
        Check if a host string is an IPv6 address.

        Simple heuristic: contains colons and is not obviously IPv4 or hostname.
        """
        # If it contains colons and is not obviously IPv4 mapped or a hostname
        if ":" in host:
            # Not IPv4-mapped (like ::ffff:192.0.2.1)
            # Not obviously a hostname (no dots except in IPv4-mapped case)
            if not re.match(r"^[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$", host):  # Not a hostname
                return True
        return False

    def create_listener(self, handler: THandler) -> IListener:  # type: ignore[override]
        """Create a WebSocket listener."""
        logger.debug("WebsocketTransport.create_listener called")
        return WebsocketListener(handler, self._upgrader)
