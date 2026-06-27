import sys
import os
import socket
import struct
import questionary
import time

# Adicionar a pasta client ao sys.path para conseguirmos importar messages_pb2 gerado pelo protoc
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(BASE_DIR, 'client'))

try:
    import messages_pb2 # pyright: ignore[reportMissingImports]
except ImportError:
    print("messages_pb2 não encontrado. Certifique-se de compilar o Protobuf primeiro ou rodar o client via Docker.")
    sys.exit(1)

GATEWAY_HOST = "127.0.0.1"
GATEWAY_PORT = 5001

def recv_exact(sock: socket.socket, size: int) -> bytes:
    data = bytearray()
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise RuntimeError("Conexão fechada pelo gateway de forma inesperada")
        data.extend(chunk)
    return bytes(data)

def send_request(req: messages_pb2.ClientRequest) -> messages_pb2.ClientResponse:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5.0)
    try:
        sock.connect((GATEWAY_HOST, GATEWAY_PORT))
        msg_data = req.SerializeToString()
        frame = struct.pack(">I", len(msg_data)) + msg_data
        sock.sendall(frame)
        
        header = recv_exact(sock, 4)
        resp_len = struct.unpack(">I", header)[0]
        data = recv_exact(sock, resp_len)
        resp = messages_pb2.ClientResponse()
        resp.ParseFromString(data)
        return resp
    finally:
        sock.close()

def list_devices():
    print("\nConsultando gateway...")
    req = messages_pb2.ClientRequest()
    req.type = messages_pb2.REQUEST_TYPE_LIST_DEVICES
    try:
        resp = send_request(req)
        if resp.success:
            print(f"\n✅ {len(resp.devices)} dispositivos encontrados:")
            for d in resp.devices:
                status = "ONLINE" if d.status == messages_pb2.STATUS_ON else "OFFLINE"
                ctrl = "Controlável" if d.is_controllable else "Apenas leitura"
                print(f" - [{status}] ID: {d.device_id} | IP: {d.ip_address}:{d.control_port} ({ctrl})")
        else:
            print(f"❌ Erro do Gateway: {resp.message}")
    except Exception as e:
        print(f"❌ Falha na comunicação: {e}")
    print()

def main():
    questionary.print("Bem-vindo ao Painel de Administração da Smart City (CLI)", style="bold italic fg:cyan")
    
    while True:
        action = questionary.select(
            "O que você deseja fazer?",
            choices=[
                "Listar Dispositivos",
                "Verificar Status do Gateway",
                "Sair"
            ]
        ).ask()
        
        if action == "Sair" or action is None:
            print("Saindo...")
            break
        elif action == "Listar Dispositivos":
            list_devices()
        elif action == "Verificar Status do Gateway":
            print("\nTentando ping no Gateway...")
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(2.0)
                sock.connect((GATEWAY_HOST, GATEWAY_PORT))
                sock.close()
                print("✅ Gateway está ONLINE.")
            except Exception as e:
                print(f"❌ Gateway está OFFLINE: {e}")
            print()

if __name__ == "__main__":
    main()
