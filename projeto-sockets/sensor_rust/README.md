# Sensor de água em Rust

Simula a distribuição de água em nove dispositivos virtuais por padrão, com IDs
`water_centro_01`, `water_campus_01`, `water_hospital_01` até o sufixo `_03`.
Cada dispositivo tem estado e frequência próprios, controlados por TCP na porta
5008. A frequência inicial é de cinco segundos; o controle aceita de um a sessenta
segundos e estados `STATUS_ON`/`STATUS_OFF`.

| Métrica | Unidade | Faixa da simulação |
| --- | --- | --- |
| `water_level` | % | 5–100 |
| `water_flow` | L/min | 5–150 |
| `water_pressure` | bar | 0,5–7 |
| `water_ph` | pH | 6–9 |
| `water_turbidity` | NTU | 0–10 |
| `water_temperature` | °C | 10–35 |
| `conductivity` | uS/cm | 50–1200 |
| `leak_rate` | L/min | 0–4 |

As leituras são simuladas, evoluem suavemente e respeitam esses limites. Um
dispositivo desligado continua enviando o estado sem leituras físicas. Os anúncios
de descoberta incluem também os dispositivos desligados para recuperação após
reiniciar o gateway ou os hubs.

## Compilar e testar

Com Rust 1.90 ou superior, execute nesta pasta:

```sh
cargo test --locked
cargo build --release --locked
```

O `build.rs` gera os bindings de Protobuf diretamente de
`../common/messages.proto`; o compilador `protoc` vem das dependências travadas.
Para o build em Docker, use a pasta `projeto-sockets` como contexto:

```sh
docker build --target builder -f sensor_rust/Dockerfile .
docker build -t smartcity-water -f sensor_rust/Dockerfile .
```

O primeiro comando executa os testes e produz o binário. A imagem final usa
`/app/sensor-water`, usuário 65532 e o subcomando `healthcheck` para verificar a
porta de controle.

## Configuração

| Variável | Padrão | Uso |
| --- | --- | --- |
| `GATEWAY_HOST` | `gateway` | Destino da ingestão; use `hub_sensores` na topologia com hub |
| `SENSOR_HOSTNAME` | `sensor_agua` | Nome anunciado para controle |
| `RUST_WATER_DEVICE_COUNT` | `9` | Quantidade de dispositivos, de 1 a 100 |
| `SENSOR_HEARTBEAT_INTERVAL_SECS` | `10` | Intervalo dos anúncios, de 1 a 3600 s |
| `SENSOR_HEARTBEAT_JITTER_SECS` | `2` | Variação dos anúncios, de 0 a 60 s |
| `SENSOR_DISCOVERY_JITTER_SECS` | `2` | Variação da resposta ao probe, de 0 a 30 s |
| `SENSOR_MULTICAST_ENABLED` | `true` | Escuta de probes em 239.0.0.1:5005 |
| `SENSOR_CONTROL_PORT` | `5008` | Porta de controle e verificação de saúde |
| `GATEWAY_TELEMETRY_PORT` | `5000` | Destino UDP de telemetria |
| `GATEWAY_DISCOVERY_PORT` | `5002` | Destino UDP de descoberta |
| `SENSOR_HEALTHCHECK_ADDRESS` | `127.0.0.1:5008` | Endereço opcional de verificação de saúde |

O sensor aceita frames TCP com um prefixo de quatro bytes big-endian e Protobuf,
limitados a 1 MiB. Rejeita alvo desconhecido, ID inválido, timestamp com mais de
300 segundos de idade ou mais de 60 segundos no futuro e alterações inválidas.
Uma proteção de replay compartilhada entre as conexões impede reaplicar um
`command_id` por dez minutos. Limites de clientes, tempo de leitura e tamanho de
frame protegem o servidor contra conexões incompletas. No encerramento por sinal,
o processo para os trabalhadores e anuncia `STATUS_OFF` para a frota.
