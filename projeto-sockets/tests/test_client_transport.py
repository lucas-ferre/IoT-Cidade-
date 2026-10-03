"""Regressões do framing e da correlação das respostas TCP do cliente."""

from __future__ import annotations

import socket
import struct
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


CLIENT_DIR = Path(__file__).resolve().parents[1] / "client"
sys.path.insert(0, str(CLIENT_DIR))

import messages_pb2  # noqa: E402
from gateway_transport import GatewayTcpClient, _MAX_TCP_FRAME_BYTES  # noqa: E402


def framed_response(request_id: str, *, success: bool = True) -> bytes:
    body = messages_pb2.ClientResponse(message_id=request_id, success=success).SerializeToString()
    return struct.pack(">I", len(body)) + body


class FakeSocket:
    def __init__(
        self, incoming: bytes = b"", *, chunk_size: int = 4096,
        connect_error: OSError | None = None, receive_error: OSError | None = None,
    ):
        self.incoming = incoming
        self.chunk_size = chunk_size
        self.connect_error = connect_error
        self.receive_error = receive_error
        self.sent: list[bytes] = []
        self.closed = False
        self.connected_to = None

    def settimeout(self, timeout: float) -> None:
        pass

    def setsockopt(self, *args) -> None:
        pass

    def connect(self, address) -> None:
        if self.connect_error:
            raise self.connect_error
        self.connected_to = address

    def sendall(self, payload: bytes) -> None:
        self.sent.append(payload)

    def recv(self, size: int) -> bytes:
        if self.receive_error:
            raise self.receive_error
        length = min(size, self.chunk_size, len(self.incoming))
        chunk = self.incoming[:length]
        self.incoming = self.incoming[length:]
        return chunk

    def close(self) -> None:
        self.closed = True


class GatewayTransportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.request = messages_pb2.ClientRequest(
            message_id="REQ-transport-test", timestamp=123,
            type=messages_pb2.REQUEST_TYPE_LIST_DEVICES,
        )
        self.client = GatewayTcpClient("gateway.test", 5001)
        self.addCleanup(self.client.close)

    def test_reassembles_fragmented_header_and_payload(self) -> None:
        sock = FakeSocket(framed_response(self.request.message_id), chunk_size=1)
        with patch("gateway_transport.socket.socket", return_value=sock):
            result = self.client.request(self.request)

        self.assertTrue(result.transport_ok)
        self.assertTrue(result.response.success)
        self.assertEqual(sock.connected_to, ("gateway.test", 5001))
        body = self.request.SerializeToString()
        self.assertEqual(sock.sent, [struct.pack(">I", len(body)) + body])

    def test_persistent_socket_matches_each_request_separately(self) -> None:
        sock = FakeSocket(framed_response(self.request.message_id) + framed_response("REQ-next"))
        with patch("gateway_transport.socket.socket", return_value=sock) as factory:
            first = self.client.request(self.request)
            self.request.message_id = "REQ-next"
            second = self.client.request(self.request)

        self.assertTrue(first.transport_ok)
        self.assertTrue(second.transport_ok)
        self.assertEqual(second.response.message_id, "REQ-next")
        self.assertEqual(factory.call_count, 1)

    def test_rejects_response_for_another_request_and_discards_connection(self) -> None:
        sock = FakeSocket(framed_response("REQ-other"))
        with patch("gateway_transport.socket.socket", return_value=sock):
            result = self.client.request(self.request)

        self.assertEqual(result.error_code, "RESPONSE_ID_MISMATCH")
        self.assertIsNone(result.response)
        self.assertTrue(sock.closed)
        self.assertIsNone(self.client._sock)

    def test_rejects_response_without_correlation_id(self) -> None:
        sock = FakeSocket(framed_response(""))
        with patch("gateway_transport.socket.socket", return_value=sock):
            result = self.client.request(self.request)

        self.assertEqual(result.error_code, "RESPONSE_ID_MISMATCH")
        self.assertTrue(sock.closed)

    def test_gateway_rejection_keeps_transport_success_when_id_matches(self) -> None:
        sock = FakeSocket(framed_response(self.request.message_id, success=False))
        with patch("gateway_transport.socket.socket", return_value=sock):
            result = self.client.request(self.request)

        self.assertTrue(result.transport_ok)
        self.assertFalse(result.response.success)

    def test_invalid_lengths_close_the_connection_before_reading_payload(self) -> None:
        for size in (0, _MAX_TCP_FRAME_BYTES + 1):
            with self.subTest(size=size):
                sock = FakeSocket(struct.pack(">I", size))
                with patch("gateway_transport.socket.socket", return_value=sock):
                    result = self.client.request(self.request)
                self.assertEqual(result.error_code, "INVALID_FRAME_SIZE")
                self.assertTrue(sock.closed)

    def test_invalid_protobuf_payload_is_rejected(self) -> None:
        payload = b"\xff"
        sock = FakeSocket(struct.pack(">I", len(payload)) + payload)
        with patch("gateway_transport.socket.socket", return_value=sock):
            result = self.client.request(self.request)

        self.assertEqual(result.error_code, "PROTOBUF_DECODE_ERROR")
        self.assertTrue(sock.closed)

    def test_reconnects_once_after_stale_persistent_connection(self) -> None:
        stale_sock = FakeSocket()
        new_sock = FakeSocket(framed_response(self.request.message_id))
        with patch("gateway_transport.socket.socket", side_effect=[stale_sock, new_sock]) as factory:
            result = self.client.request(self.request)

        self.assertTrue(result.transport_ok)
        self.assertTrue(stale_sock.closed)
        self.assertEqual(factory.call_count, 2)

    def test_reconnect_failure_is_reported_after_two_closed_connections(self) -> None:
        sockets = [FakeSocket(), FakeSocket()]
        with patch("gateway_transport.socket.socket", side_effect=sockets) as factory:
            result = self.client.request(self.request)

        self.assertEqual(result.error_code, "CONNECTION_CLOSED")
        self.assertEqual(factory.call_count, 2)
        self.assertTrue(all(sock.closed for sock in sockets))

    def test_io_timeout_discards_connection(self) -> None:
        sock = FakeSocket(receive_error=socket.timeout())
        with patch("gateway_transport.socket.socket", return_value=sock):
            result = self.client.request(self.request)

        self.assertEqual(result.error_code, "IO_TIMEOUT")
        self.assertTrue(sock.closed)

    def test_connection_refused_closes_socket(self) -> None:
        sock = FakeSocket(connect_error=ConnectionRefusedError())
        with patch("gateway_transport.socket.socket", return_value=sock):
            result = self.client.request(self.request)

        self.assertEqual(result.error_code, "CONNECTION_REFUSED")
        self.assertTrue(sock.closed)

    def test_status_probe_does_not_interfere_with_in_flight_request(self) -> None:
        self.client._lock.acquire()
        try:
            with patch("gateway_transport.socket.socket") as factory:
                result = self.client.ensure_connected()
        finally:
            self.client._lock.release()

        self.assertEqual(result.error_code, "BUSY")
        factory.assert_not_called()


if __name__ == "__main__":
    unittest.main()
