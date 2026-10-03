# Sensor invasor de teste

Este emissor produz cenários limitados de mensagens inválidas para verificar os
dois hubs do laboratório. Ele só executa quando `INTRUDER_ENABLED=1`, usado pelo
perfil Compose `security-test`:

```sh
docker compose --profile security-test run --rm sensor_invasor
docker compose logs hub_sensores hub_acesso
```

Uma rodada emite nove datagramas UDP e três pedidos TCP: identidade desconhecida,
descoberta desconhecida, falsificação da identidade de uma câmera, Protobuf
malformado, timestamp antigo ou futuro, métrica NaN, datagrama acima do limite,
tipo de pedido inválido e frequência de comando fora do intervalo. Nenhum cenário
envia um comando válido capaz de alterar sensores legítimos.

Os destinos são somente os dois hosts explicitamente configurados e precisam
resolver para IPv4 privado ou loopback. O emissor não procura dispositivos nem
varre portas. Por padrão, executa uma rodada com intervalo de 0,15 segundo entre
mensagens e encerra. A configuração limita a dez rodadas e proíbe intervalos
abaixo de 0,02 segundo.

| Variável | Padrão |
|---|---|
| `INTRUDER_ENABLED` | `0` fora do perfil Compose |
| `INTRUDER_SENSOR_HUB_HOST` | `hub_sensores` |
| `INTRUDER_ACCESS_HUB_HOST` | `hub_acesso` |
| `INTRUDER_TELEMETRY_PORT` | `5000` |
| `INTRUDER_DISCOVERY_PORT` | `5002` |
| `INTRUDER_ACCESS_PORT` | `5001` |
| `INTRUDER_SPOOF_DEVICE_ID` | `camera_pici_01` |
| `INTRUDER_ROUNDS` | `1` |
| `INTRUDER_INTERVAL_SECS` | `0.15` |
| `INTRUDER_UDP_MAX_BYTES` | `16384`, deve corresponder ao limite do hub |

Os logs indicam o envio UDP como `sent_not_acknowledged`: o transporte não oferece
confirmação e o resultado de bloqueio é verificado na auditoria do hub. Para TCP,
o emissor distingue resposta negativa, conexão fechada pelo hub, sucesso
inesperado e erro de transporte. A saída do processo é zero quando os cenários
foram emitidos e os pedidos TCP foram rejeitados; erros de transporte ou respostas
inesperadas produzem saída um.

Os testes de integração usam sockets locais e comprovam que nenhum dos cenários
alcança o destino UDP do gateway nem gera um registro de dispositivo no hub.
