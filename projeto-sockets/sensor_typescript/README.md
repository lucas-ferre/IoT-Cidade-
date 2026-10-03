# Sensor de lixeiras em TypeScript

Executa diretamente em **Node.js 22.18 ou superior**, com remoção nativa dos tipos.
Não possui dependências npm nem exige compilador TypeScript. A frota padrão
contém **9 dispositivos**, distribuídos entre Centro, Campus e Hospital, com IDs
`waste_centro_01` até `waste_hospital_03` e **9 métricas por leitura**.

O protocolo usa `DEVICE_TYPE_WASTE_SENSOR = 8`. O sensor envia descobertas
Protobuf por UDP para a porta 5002 e telemetria para a porta 5000. Na configuração
Compose, `GATEWAY_HOST` aponta para `hub_sensores`. O padrão do código é `gateway`.
As lixeiras anunciam `is_controllable=false` e `control_port=0`.

| Métrica | Unidade | Significado |
|---|---|---|
| `waste_fill_level` | `%` | Ocupação da lixeira |
| `waste_weight` | `kg` | Peso proporcional à ocupação; capacidade simulada de 120 kg |
| `bin_temperature` | `C` | Temperatura interna |
| `bin_humidity` | `%` | Umidade interna |
| `battery_level` | `%` | Bateria restante |
| `signal_strength` | `dBm` | Intensidade do sinal; valores negativos são esperados |
| `collection_count` | `count` | Coletas acumuladas por dispositivo desde o início do processo |
| `tilt_angle` | `deg` | Inclinação |
| `fire_risk` | `%` | Indicador simulado de aquecimento |

Ao atingir 95% de ocupação, a simulação incrementa `collection_count` e esvazia a
lixeira para uma ocupação entre 5% e 15%. Peso e ocupação permanecem proporcionais,
e a bateria fica entre 0% e 100%. Reiniciar o processo também reinicia as coletas.

## Execução e configuração

Execute os comandos abaixo na pasta `sensor_typescript`:

```sh
node --experimental-strip-types src/main.ts
```

| Variável de ambiente | Padrão | Valores aceitos |
|---|---|---|
| `GATEWAY_HOST` | `gateway` | Hostname ou endereço IPv4, até 253 caracteres |
| `SENSOR_HOSTNAME` | `sensor_lixeiras` | Identificação anunciada como hostname ou IPv4 |
| `TS_WASTE_DEVICE_COUNT` | `9` | Inteiro de 1 a 100 |
| `GATEWAY_TELEMETRY_PORT` | `5000` | Inteiro de 1 a 65535 |
| `GATEWAY_DISCOVERY_PORT` | `5002` | Inteiro de 1 a 65535 |
| `SENSOR_TELEMETRY_INTERVAL_SECS` | `5` | De 0,1 a 3600 segundos |
| `SENSOR_HEARTBEAT_INTERVAL_SECS` | `10` | De 1 a 3600 segundos |
| `TS_UDP_SEND_TIMEOUT_SECS` | `1` | De 0,05 a 10 segundos |
| `SENSOR_SHUTDOWN_TIMEOUT_SECS` | `3` | De 0,1 a 30 segundos |

Os valores numéricos precisam ser finitos. Campos vazios e valores fora dos
limites encerram a inicialização com erro. Nos valores fracionários das variáveis,
use ponto decimal, por exemplo `0.5`.

O sensor anuncia todos os dispositivos antes do primeiro lote de telemetria e
renova a descoberta com heartbeat. A próxima rodada de cada canal só é agendada
quando o lote anterior termina; seus intervalos são medidos a partir desse fim.
Falhas de DNS ou de envio são registradas, e as rodadas posteriores voltam a tentar.

Em `SIGINT` ou `SIGTERM`, o runtime interrompe os agendamentos, aguarda os trabalhos
pendentes e tenta anunciar `STATUS_OFF` dentro do prazo de encerramento. Depois,
cancela os temporizadores de envios ainda pendentes e fecha o socket. UDP não
oferece confirmação de entrega; a auditoria do hub permite verificar o recebimento.

## Testes

```sh
node --experimental-strip-types --test tests/*.test.ts
python -m unittest discover -s tests -p 'test_*.py' -v
```

São **9 testes Node** de frota, invariantes, configurações, UDP real em loopback,
rodadas sem sobreposição, falhas de envio e encerramento. Os **2 testes Python**
decodificam os frames TypeScript usando o contrato Protobuf compartilhado, incluindo
timestamp acima de 32 bits, `double` negativo e texto UTF-8. Eles exigem o runtime
Python `protobuf` e `protoc`; quando `protoc` não está disponível, usam
`grpc_tools.protoc`. A variável opcional `NODE_BINARY` indica o executável Node
caso ele não esteja no `PATH`.

O [catálogo geral](../../docs/metrics.md) descreve as métricas de todas as famílias.
