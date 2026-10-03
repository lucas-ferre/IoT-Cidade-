"""Transporte TCP/Protobuf do cliente, independente da interface Streamlit."""

import re
import socket
import struct
import threading
from dataclasses import dataclass

from google.protobuf.message import DecodeError

import messages_pb2

_TCP_CONNECT_TIMEOUT = 2.0
_TCP_IO_TIMEOUT = 5.0
_MAX_TCP_FRAME_BYTES = 1024 * 1024
_OS_ERROR_CODE_RE = re.compile(r"\[(?:Errno|WinError)\s*-?\d+\]\s*|\bErrno\s*-?\d+\b:?\s*", re.IGNORECASE)
_GATEWAY_UNAVAILABLE_CODES = {
    "GATEWAY_DNS_ERROR",
    "CONNECT_TIMEOUT",
    "CONNECTION_REFUSED",
    "CONNECTION_CLOSED",
    "IO_TIMEOUT",
    "SOCKET_ERROR",
}


def _clean_os_error_text(exc: BaseException) -> str:
    message = ""
    if isinstance(exc, OSError):
        message = getattr(exc, "strerror", "") or ""
        if not message and len(exc.args) > 1 and isinstance(exc.args[1], str):
            message = exc.args[1]

    if not message:
        message = str(exc)

    message = _OS_ERROR_CODE_RE.sub("", message).strip()
    return message or exc.__class__.__name__


@dataclass
class TcpRequestResult:
    response: messages_pb2.ClientResponse | None = None
    error_code: str = ""
    error_message: str = ""

    @property
    def transport_ok(self) -> bool:
        return self.error_code == ""


class GatewayConnectionClosed(RuntimeError):
    pass


class GatewayTcpClient:
    """Cliente TCP persistente para reduzir handshakes e classificar falhas de rede."""

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self._sock: socket.socket | None = None
        self._lock = threading.Lock()

    @property
    def is_busy(self) -> bool:
        return self._lock.locked()

    def close(self) -> None:
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None

    def _connect_locked(self) -> None:
        if self._sock is not None:
            return

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.settimeout(_TCP_CONNECT_TIMEOUT)
            sock.connect((self.host, self.port))
            sock.settimeout(_TCP_IO_TIMEOUT)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self._sock = sock
        except (socket.timeout, ConnectionRefusedError, OSError):
            sock.close()
            raise

    def ensure_connected(self) -> TcpRequestResult:
        acquired = self._lock.acquire(blocking=False)
        if not acquired:
            return TcpRequestResult(
                error_code="BUSY",
                error_message="Canal TCP ocupado por outra operação em andamento.",
            )

        try:
            try:
                self._connect_locked()
                return TcpRequestResult()
            except socket.timeout:
                self.close()
                return TcpRequestResult(
                    error_code="CONNECT_TIMEOUT",
                    error_message=f"Tempo limite ao abrir conexão TCP com {self.host}:{self.port}.",
                )
            except socket.gaierror as exc:
                self.close()
                return TcpRequestResult(
                    error_code="GATEWAY_DNS_ERROR",
                    error_message=(
                        f"Não foi possível resolver o host '{self.host}'. "
                        f"Detalhe: {_clean_os_error_text(exc)}."
                    ),
                )
            except ConnectionRefusedError:
                self.close()
                return TcpRequestResult(
                    error_code="CONNECTION_REFUSED",
                    error_message=f"Conexão TCP recusada em {self.host}:{self.port}.",
                )
            except OSError as exc:
                self.close()
                return TcpRequestResult(
                    error_code="SOCKET_ERROR",
                    error_message=(
                        f"Falha de socket ao conectar a {self.host}:{self.port}. "
                        f"Detalhe: {_clean_os_error_text(exc)}."
                    ),
                )
        finally:
            self._lock.release()

    def _recv_exact_locked(self, size: int) -> bytes:
        assert self._sock is not None, "Socket não inicializado em _recv_exact_locked"
        data = bytearray()
        while len(data) < size:
            chunk = self._sock.recv(size - len(data))
            if not chunk:
                raise GatewayConnectionClosed("Gateway encerrou a conexão TCP.")
            data.extend(chunk)
        return bytes(data)

    def request(self, request: messages_pb2.ClientRequest) -> TcpRequestResult:
        msg_data = request.SerializeToString()
        frame = struct.pack(">I", len(msg_data)) + msg_data

        with self._lock:
            for attempt in range(2):
                try:
                    self._connect_locked()
                    assert self._sock is not None, "Socket não inicializado após _connect_locked"
                    self._sock.sendall(frame)

                    header = self._recv_exact_locked(4)
                    resp_len = struct.unpack(">I", header)[0]
                    if resp_len <= 0 or resp_len > _MAX_TCP_FRAME_BYTES:
                        self.close()
                        return TcpRequestResult(
                            error_code="INVALID_FRAME_SIZE",
                            error_message=f"Gateway retornou frame inválido: {resp_len} bytes.",
                        )

                    data = self._recv_exact_locked(resp_len)
                    response = messages_pb2.ClientResponse()
                    response.ParseFromString(data)
                    if response.message_id != request.message_id:
                        self.close()
                        return TcpRequestResult(
                            error_code="RESPONSE_ID_MISMATCH",
                            error_message=(
                                "Gateway retornou uma resposta vinculada a outra requisição "
                                f"(esperado '{request.message_id}', recebido '{response.message_id}')."
                            ),
                        )
                    return TcpRequestResult(response=response)

                except (GatewayConnectionClosed, ConnectionResetError, ConnectionAbortedError, BrokenPipeError) as exc:
                    self.close()
                    if attempt == 0:
                        continue
                    return TcpRequestResult(
                        error_code="CONNECTION_CLOSED",
                        error_message=(
                            "Conexão TCP encerrada durante a requisição. "
                            f"Detalhe: {_clean_os_error_text(exc)}."
                        ),
                    )
                except socket.timeout:
                    self.close()
                    return TcpRequestResult(
                        error_code="IO_TIMEOUT",
                        error_message="Gateway não respondeu dentro da janela de timeout do socket.",
                    )
                except socket.gaierror as exc:
                    self.close()
                    return TcpRequestResult(
                        error_code="GATEWAY_DNS_ERROR",
                        error_message=(
                            f"Não foi possível resolver o host '{self.host}'. "
                            f"Detalhe: {_clean_os_error_text(exc)}."
                        ),
                    )
                except ConnectionRefusedError:
                    self.close()
                    return TcpRequestResult(
                        error_code="CONNECTION_REFUSED",
                        error_message=f"Conexão TCP recusada em {self.host}:{self.port}.",
                    )
                except struct.error as exc:
                    self.close()
                    return TcpRequestResult(
                        error_code="FRAME_HEADER_ERROR",
                        error_message=f"Cabeçalho de frame TCP corrompido: {exc}",
                    )
                except DecodeError as exc:
                    self.close()
                    return TcpRequestResult(
                        error_code="PROTOBUF_DECODE_ERROR",
                        error_message=f"Resposta Protobuf inválida do Gateway: {exc}",
                    )
                except OSError as exc:
                    self.close()
                    return TcpRequestResult(
                        error_code="SOCKET_ERROR",
                        error_message=f"Falha de socket no fluxo TCP: {_clean_os_error_text(exc)}.",
                    )

        return TcpRequestResult(
            error_code="REQUEST_FAILED",
            error_message="Falha não classificada no fluxo TCP.",
        )


