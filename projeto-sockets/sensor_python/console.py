"""Console de controle interativo da Câmera de Tráfego (Python).

Modos de uso:

1. STANDALONE (processo separado) — conecta na porta de controle TCP do sensor
   e envia ConfigCommand (mesmo contrato usado pelo Gateway/Dashboard):

       docker exec -it sensor_camera python console.py
       # ou apontando para outro host/porta:
       python console.py <host> <porta>

2. EMBUTIDO (IDLE no próprio processo do sensor) — o sensor.py inicia este
   console em uma thread quando a variável SENSOR_IDLE_CONSOLE está ligada.
   Requer um terminal anexado (docker compose: stdin_open + tty):

       docker attach sensor_camera        # Ctrl-P Ctrl-Q para desanexar

Ambos os modos falam o protocolo length-prefix (>I) + Protobuf ConfigCommand,
abrindo uma conexão por comando (o servidor de controle atende um frame por
conexão). Comandos disponíveis: status, on, off, err, freq, help, quit.
"""

import os
import socket
import struct
import sys
import time
import uuid

import messages_pb2  # pyright: ignore[reportMissingImports]
import control_crypto

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 5004  # CONTROL_TCP_PORT da câmera Python
MAX_FRAME_SIZE = 1024 * 1024
IO_TIMEOUT = 5.0

_STATUS_LABEL = {
    messages_pb2.STATUS_ON: "ON",
    messages_pb2.STATUS_OFF: "OFF",
    messages_pb2.STATUS_ERROR: "ERROR",
    messages_pb2.STATUS_UNKNOWN: "UNKNOWN",
}


def _recv_exact(sock: socket.socket, size: int) -> bytes:
    data = bytearray()
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise ConnectionError("conexão encerrada antes do frame completo")
        data.extend(chunk)
    return bytes(data)


def send_command(
    host: str,
    port: int,
    *,
    update_status: bool = False,
    target_status: int = messages_pb2.STATUS_ON,
    update_frequency: bool = False,
    new_frequency_secs: int = 0,
    target_device_id: str = "",
) -> messages_pb2.ConfigResponse:
    """Abre uma conexão, envia um ConfigCommand e devolve o ConfigResponse."""
    cmd = messages_pb2.ConfigCommand(
        command_id=f"CONSOLE-{uuid.uuid4().hex[:6].upper()}",
        timestamp=int(time.time()),
        update_status=update_status,
        target_status=target_status,
        update_frequency=update_frequency,
        new_frequency_secs=new_frequency_secs,
        target_device_id=target_device_id,
    )
    payload = cmd.SerializeToString()
    if control_crypto.SECURE:
        payload = control_crypto.wrap(payload)

    with socket.create_connection((host, port), timeout=IO_TIMEOUT) as sock:
        sock.settimeout(IO_TIMEOUT)
        sock.sendall(struct.pack(">I", len(payload)) + payload)

        header = _recv_exact(sock, 4)
        resp_len = struct.unpack(">I", header)[0]
        if resp_len <= 0 or resp_len > MAX_FRAME_SIZE:
            raise ValueError(f"frame de resposta inválido: {resp_len} bytes")
        body = _recv_exact(sock, resp_len)

    if control_crypto.SECURE:
        body = control_crypto.unwrap(body)
    resp = messages_pb2.ConfigResponse()
    resp.ParseFromString(body)
    return resp


def _print_response(resp: messages_pb2.ConfigResponse) -> None:
    ok = "✓" if resp.success else "✗"
    status = _STATUS_LABEL.get(resp.updated_status, str(resp.updated_status))
    print(
        f"  {ok} {resp.message or '(sem mensagem)'}\n"
        f"    status={status} | frequência={resp.updated_frequency_secs}s | cmd={resp.command_id}"
    )


HELP_TEXT = """
Comandos do console (câmera Python):
  status [device_id]        Lê o estado atual (não altera nada).
  on     [device_id]        Liga o dispositivo (STATUS_ON).
  off    [device_id]        Desliga o dispositivo (STATUS_OFF).
  err    [device_id]        Marca falha (STATUS_ERROR).
  freq <segundos> [device_id]   Altera o intervalo de telemetria.
  help                      Mostra esta ajuda.
  quit / exit               Sai do console.

Sem device_id, o sensor aplica ao dispositivo padrão (primeiro da frota).
""".strip()


def _dispatch(host: str, port: int, line: str) -> bool:
    """Processa uma linha do REPL. Retorna False para encerrar o console."""
    parts = line.split()
    if not parts:
        return True
    cmd, args = parts[0].lower(), parts[1:]

    try:
        if cmd in ("quit", "exit"):
            return False
        if cmd in ("help", "?"):
            print(HELP_TEXT)
            return True
        if cmd == "status":
            device_id = args[0] if args else ""
            _print_response(send_command(host, port, target_device_id=device_id))
            return True
        if cmd in ("on", "off", "err"):
            target = {
                "on": messages_pb2.STATUS_ON,
                "off": messages_pb2.STATUS_OFF,
                "err": messages_pb2.STATUS_ERROR,
            }[cmd]
            device_id = args[0] if args else ""
            _print_response(send_command(
                host, port, update_status=True, target_status=target, target_device_id=device_id,
            ))
            return True
        if cmd == "freq":
            if not args or not args[0].isdigit() or int(args[0]) <= 0:
                print("  Uso: freq <segundos> [device_id]  (segundos > 0)")
                return True
            secs = int(args[0])
            device_id = args[1] if len(args) > 1 else ""
            _print_response(send_command(
                host, port, update_frequency=True, new_frequency_secs=secs, target_device_id=device_id,
            ))
            return True

        print(f"  Comando desconhecido: '{cmd}'. Digite 'help'.")
    except (OSError, ValueError, ConnectionError) as exc:
        print(f"  ✗ Falha de comunicação com {host}:{port}: {exc}")
    return True


def run_console(host: str = DEFAULT_HOST, port: int = DEFAULT_PORT, *, prompt: str = "camera> ") -> None:
    """Loop interativo (IDLE). Usado tanto no modo standalone quanto no embutido."""
    print("============================================================")
    print(f"[Console Python] Interface de controle da câmera ({host}:{port}).")
    print("[Console Python] Digite 'help' para ver os comandos, 'quit' para sair.")
    print("============================================================")
    while True:
        try:
            line = input(prompt)
        except (EOFError, KeyboardInterrupt):
            print("\n[Console Python] Encerrando console.")
            return
        if not _dispatch(host, port, line):
            print("[Console Python] Console encerrado.")
            return


def start_embedded(port: int = DEFAULT_PORT) -> None:
    """Inicia o console IDLE em thread daemon (chamado pelo sensor)."""
    import threading

    threading.Thread(
        target=run_console,
        kwargs={"host": "127.0.0.1", "port": port},
        daemon=True,
        name="idle-console",
    ).start()


if __name__ == "__main__":
    cli_host = sys.argv[1] if len(sys.argv) > 1 else os.getenv("SENSOR_CONSOLE_HOST", DEFAULT_HOST)
    cli_port = int(sys.argv[2]) if len(sys.argv) > 2 else int(os.getenv("SENSOR_CONSOLE_PORT", str(DEFAULT_PORT)))
    run_console(cli_host, cli_port)
