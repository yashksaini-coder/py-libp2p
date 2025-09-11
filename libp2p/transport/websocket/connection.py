"""WebSocket connection wrapper for libp2p."""

import logging
from typing import Any

import trio

from libp2p.io.abc import ReadWriteCloser
from libp2p.io.exceptions import IOException

logger = logging.getLogger(__name__)


class P2PWebSocketConnection(ReadWriteCloser):
    """
    Wraps a WebSocketConnection to provide the raw stream interface
    that libp2p protocols expect.

    This class provides byte-level access to WebSocket messages,
    which is required for security protocols like Noise handshake.
    """

    def __init__(self, ws_connection: Any, ws_context: Any | None = None) -> None:
        self._ws_connection = ws_connection
        self._ws_context = ws_context
        self._read_buffer = b""
        self._read_lock = trio.Lock()
        self._closed = False

    async def write(self, data: bytes) -> None:
        """Write data to the WebSocket as a binary message."""
        if self._closed:
            raise IOException("Connection is closed")

        try:
            logger.debug(f"WebSocket writing {len(data)} bytes")
            # Send as a binary WebSocket message
            await self._ws_connection.send_message(data)
            logger.debug(f"WebSocket wrote {len(data)} bytes successfully")
        except Exception as e:
            logger.error(f"WebSocket write failed: {e}")
            raise IOException from e

    async def read(self, n: int | None = None) -> bytes:
        """
        Read up to n bytes (if n is given), else read up to 64KiB.

        This implementation provides byte-level access to WebSocket messages,
        which is required for Noise protocol handshake and other libp2p protocols.
        """
        if self._closed:
            return b""

        async with self._read_lock:
            try:
                logger.debug(
                    f"WebSocket read requested: n={n}, "
                    f"buffer_size={len(self._read_buffer)}"
                )

                # If we have buffered data and don't need a specific amount, return it
                if self._read_buffer and n is None:
                    result = self._read_buffer
                    self._read_buffer = b""
                    logger.debug(
                        f"WebSocket read returning all buffered data: "
                        f"{len(result)} bytes"
                    )
                    return result

                # If we have enough buffered data to satisfy the request
                if n is not None and len(self._read_buffer) >= n:
                    result = self._read_buffer[:n]
                    self._read_buffer = self._read_buffer[n:]
                    logger.debug(
                        f"WebSocket read returning {len(result)} bytes from buffer"
                    )
                    return result

                # We need more data, get it from WebSocket
                while n is None or len(self._read_buffer) < n:
                    logger.debug(
                        f"WebSocket read getting more data: "
                        f"buffer_size={len(self._read_buffer)}, need={n}"
                    )

                    try:
                        # Get the next WebSocket message
                        message = await self._ws_connection.get_message()
                    except Exception as e:
                        logger.debug(f"WebSocket read failed to get message: {e}")
                        # If we can't get more data, return what we have
                        result = self._read_buffer
                        self._read_buffer = b""
                        return result

                    if isinstance(message, str):
                        message = message.encode("utf-8")

                    logger.debug(
                        f"WebSocket read received message: {len(message)} bytes"
                    )
                    # Add to buffer
                    self._read_buffer += message

                    # If we don't need a specific amount, we can return now
                    if n is None:
                        result = self._read_buffer
                        self._read_buffer = b""
                        logger.debug(
                            f"WebSocket read returning all data: {len(result)} bytes"
                        )
                        return result

                # Return requested amount
                if n is not None:
                    result = self._read_buffer[:n]
                    self._read_buffer = self._read_buffer[n:]
                    logger.debug(f"WebSocket read returning exact {len(result)} bytes")
                    return result
                else:
                    # This shouldn't happen based on the logic above
                    result = self._read_buffer
                    self._read_buffer = b""
                    logger.debug(
                        f"WebSocket read returning remaining {len(result)} bytes"
                    )
                    return result

            except Exception as e:
                logger.error(f"WebSocket read failed: {e}")
                raise IOException from e

    async def close(self) -> None:
        """Close the WebSocket connection."""
        if self._closed:
            return

        self._closed = True
        try:
            # Close the WebSocket connection
            await self._ws_connection.aclose()
            # Exit the context manager if we have one
            if self._ws_context is not None:
                await self._ws_context.__aexit__(None, None, None)
        except Exception as e:
            logger.debug(f"Error closing WebSocket connection: {e}")

    def get_remote_address(self) -> tuple[str, int] | None:
        """Get the remote address of the WebSocket connection."""
        try:
            # Try to get remote address from the WebSocket connection
            if hasattr(self._ws_connection, "remote"):
                remote = self._ws_connection.remote
                if hasattr(remote, "address") and hasattr(remote, "port"):
                    return str(remote.address), int(remote.port)
                elif isinstance(remote, str):
                    # Parse address:port format
                    if ":" in remote:
                        # Handle IPv6 addresses in brackets
                        if remote.startswith("[") and "]:" in remote:
                            host, port = remote.rsplit("]:", 1)
                            host = host[1:]  # Remove leading bracket
                            return host, int(port)
                        else:
                            # IPv4 or hostname
                            host, port = remote.rsplit(":", 1)
                            return host, int(port)

            # Try alternative methods to get remote address
            if hasattr(self._ws_connection, "_stream") and hasattr(
                self._ws_connection._stream, "socket"
            ):
                socket = self._ws_connection._stream.socket
                if hasattr(socket, "getpeername"):
                    peer = socket.getpeername()
                    if isinstance(peer, tuple) and len(peer) >= 2:
                        return str(peer[0]), int(peer[1])

        except Exception as e:
            logger.debug(f"Could not get remote address: {e}")

        return None
