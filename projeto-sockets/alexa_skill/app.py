import logging
import os
import socket
import struct
import redis
from flask import Flask, request, jsonify

from ask_sdk_core.skill_builder import SkillBuilder
from ask_sdk_core.dispatch_components import AbstractRequestHandler
from ask_sdk_core.dispatch_components import AbstractExceptionHandler
from ask_sdk_core.utils import is_request_type, is_intent_name
from ask_sdk_core.handler_input import HandlerInput
from ask_sdk_model import Response

import messages_pb2 # pyright: ignore[reportMissingImports]

app = Flask(__name__)
logging.getLogger().setLevel(logging.INFO)

# Configurações de conexão com o Gateway
GATEWAY_HOST = os.getenv("GATEWAY_HOST", "gateway")
GATEWAY_PORT = int(os.getenv("GATEWAY_PORT", "5001"))

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

_SEVERITY_PT = {"critical": "crítico", "warning": "alerta", "info": "informativo"}


def fetch_recent_alerts(limit: int = 3) -> str:
    """Lê os alertas mais recentes do stream Redis 'alerts' e narra por voz.

    Nota: notificações PROATIVAS (push) da Alexa exigem o Proactive Events API
    (credenciais OAuth do cliente + skill publicada). Aqui entregamos o consumo
    sob demanda; o gateway já produz os eventos no stream 'alerts'.
    """
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True,
                        socket_timeout=2.0, socket_connect_timeout=2.0)
        entries = r.xrevrange("alerts", count=limit)
        r.close()
        if not entries:
            return "Não há alertas recentes. A cidade está dentro dos limiares."
        partes = [f"Há {len(entries)} alerta(s) recente(s)."]
        for _id, f in entries:
            sev = _SEVERITY_PT.get(f.get("severity", "info"), "alerta")
            partes.append(
                f"{sev} no dispositivo {f.get('device_id', 'desconhecido')}: "
                f"{f.get('metric', '')} em {f.get('value', '')}."
            )
        return " ".join(partes)
    except Exception as exc:
        logging.error("Erro ao ler alertas do Redis: %s", exc)
        return "Não foi possível consultar os alertas no momento."

def recv_exact(sock: socket.socket, size: int) -> bytes:
    data = bytearray()
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise RuntimeError("Conexão fechada pelo gateway de forma inesperada")
        data.extend(chunk)
    return bytes(data)

def fetch_gateway_status() -> str:
    """Consulta o gateway para descobrir quantos nós estão online."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2.0)
        sock.connect((GATEWAY_HOST, GATEWAY_PORT))
        
        req = messages_pb2.ClientRequest()
        req.type = messages_pb2.REQUEST_TYPE_LIST_DEVICES
        
        msg_data = req.SerializeToString()
        frame = struct.pack(">I", len(msg_data)) + msg_data
        sock.sendall(frame)
        
        header = recv_exact(sock, 4)
        resp_len = struct.unpack(">I", header)[0]
        data = recv_exact(sock, resp_len)
        sock.close()
        
        resp = messages_pb2.ClientResponse()
        resp.ParseFromString(data)
        
        if resp.success:
            num_devices = len(resp.devices)
            online_devices = sum(1 for d in resp.devices if d.status == messages_pb2.STATUS_ON)
            return f"Atualmente temos {num_devices} sensores na rede, sendo {online_devices} online."
        return "O Gateway respondeu com erro ao consultar os sensores."
    except Exception as e:
        logging.error(f"Erro ao conectar no gateway: {e}")
        return "Não foi possível conectar ao Gateway da cidade no momento."

class LaunchRequestHandler(AbstractRequestHandler):
    def can_handle(self, handler_input):
        return is_request_type("LaunchRequest")(handler_input)

    def handle(self, handler_input):
        speech_text = "Bem-vindo ao Painel da Smart City. Você pode perguntar pelo status da rede."
        return handler_input.response_builder.speak(speech_text).set_should_end_session(False).response

class SystemStatusIntentHandler(AbstractRequestHandler):
    def can_handle(self, handler_input):
        return is_intent_name("SystemStatusIntent")(handler_input)

    def handle(self, handler_input):
        status_msg = fetch_gateway_status()
        speech_text = f"Consultando o sistema da cidade. {status_msg}"
        return handler_input.response_builder.speak(speech_text).response

class AlertsIntentHandler(AbstractRequestHandler):
    def can_handle(self, handler_input):
        return is_intent_name("AlertsIntent")(handler_input)

    def handle(self, handler_input):
        speech_text = fetch_recent_alerts()
        return handler_input.response_builder.speak(speech_text).response


class CatchAllExceptionHandler(AbstractExceptionHandler):
    def can_handle(self, handler_input, exception):
        return True

    def handle(self, handler_input, exception):
        logging.error(exception, exc_info=True)
        speech = "Desculpe, ocorreu um problema interno ao acessar o centro de controle da cidade."
        return handler_input.response_builder.speak(speech).response

sb = SkillBuilder()
sb.add_request_handler(LaunchRequestHandler())
sb.add_request_handler(SystemStatusIntentHandler())
sb.add_request_handler(AlertsIntentHandler())
sb.add_exception_handler(CatchAllExceptionHandler())

skill = sb.create()

@app.route('/', methods=['POST'])
def invoke_skill():
    # Integração básica Flask para Alexa
    payload = request.json
    headers = dict(request.headers)
    # Na vida real, seria necessário validar o certificado SSL usando o adapter do flask
    # ask_sdk_flask.adapter.SkillAdapter(skill=skill, endpoint_path="/", app=app)
    response = skill.invoke(payload, context=None)
    return jsonify(response)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5008)
