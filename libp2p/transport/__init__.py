"""Transport module for libp2p."""

from typing import TYPE_CHECKING

from .tcp.tcp import TCP
from .upgrader import TransportUpgrader
from .transport_registry import (
    TransportRegistry,
    create_transport_for_multiaddr,
    get_transport_registry,
    register_transport,
    get_supported_transport_protocols,
)

if TYPE_CHECKING:
    from libp2p.abc import ITransport

# Try to import WebSocket transport if available
try:
    from .websocket.transport import WebsocketTransport
    _websocket_available = True
except ImportError:
    _websocket_available = False
    WebsocketTransport = None  # type: ignore


def create_transport(protocol: str, upgrader: TransportUpgrader | None = None) -> "ITransport":
    """
    Convenience function to create a transport instance.

    Args:
        protocol: The transport protocol ("tcp", "ws", or custom)
        upgrader: Optional transport upgrader (required for WebSocket)

    Returns:
        Transport instance

    Raises:
        ValueError: If protocol is not supported or required arguments missing
    """
    if protocol == "tcp":
        return TCP()
    elif protocol == "ws":
        if not _websocket_available or WebsocketTransport is None:
            raise ValueError("WebSocket transport not available. Install trio-websocket: pip install trio-websocket")
        if upgrader is None:
            raise ValueError("WebSocket transport requires an upgrader")
        return WebsocketTransport(upgrader)
    else:
        # Check if it's a custom registered transport
        registry = get_transport_registry()
        transport = registry.create_transport(protocol, upgrader)
        if transport is None:
            raise ValueError(f"Unsupported transport protocol: {protocol}")
        return transport


__all__ = [
    "TCP",
    "TransportUpgrader",
    "TransportRegistry",
    "WebsocketTransport",
    "create_transport_for_multiaddr",
    "create_transport",
    "get_transport_registry",
    "register_transport",
    "get_supported_transport_protocols",
]
