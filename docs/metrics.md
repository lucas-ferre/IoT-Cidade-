# Catálogo de métricas

A frota padrão contém **66 dispositivos**, **62 nomes distintos de métricas** e
até **624 valores por rodada completa**, quando todos os dispositivos estão
ligados. Cada pacote contém também ID de mensagem, timestamp em segundos Unix, ID do
dispositivo e estado. Heartbeats de dispositivos desligados não contêm leituras.

As informações são simuladas para testar comunicação, persistência e consultas.
Indicadores como AQI, risco de aquecimento e qualidade da água não são medições
certificadas nem recomendações operacionais.

| Família | Dispositivos | Métricas por leitura | Valores por rodada |
|---|---:|---:|---:|
| C — estações ambientais | 12 | 13 | 156 |
| Lua — postes | 9 | 8 | 72 |
| Java — semáforos | 9 | 8 | 72 |
| Python — câmeras | 9 | 8 | 72 |
| Go — estacionamentos | 9 | 11 | 99 |
| Rust — água e saneamento | 9 | 8 | 72 |
| TypeScript — lixeiras | 9 | 9 | 81 |
| **Total** | **66** | **65 entradas entre famílias** | **624** |

`average_speed`, `road_occupancy` e `pedestrians_count` aparecem em semáforos e
câmeras com as mesmas unidades, resultando em 62 nomes únicos. A espera dos
semáforos usa `average_wait` em segundos; a dos estacionamentos usa
`parking_wait_time` em minutos. Essa distinção evita misturar unidades em
consultas globais. Use o filtro por dispositivo para separar famílias que
compartilham métricas.

A referência consumida pelo dashboard é
[`client/metric_catalog.py`](../projeto-sockets/client/metric_catalog.py).
Os nomes são as chaves transmitidas em `Metric.name`; as unidades são os valores
transmitidos em `Metric.unit`.

## C — estações ambientais

Serviço `sensor_clima`; 12 dispositivos padrão com IDs `estacao_*`.

| Nome no protocolo | Informação | Unidade |
|---|---|---|
| `temperature` | Temperatura ambiente | `C` |
| `humidity` | Umidade relativa | `%` |
| `co2` | CO₂ | `ppm` |
| `pm25` | Partículas PM2.5 | `ug/m3` |
| `pm10` | Partículas PM10 | `ug/m3` |
| `aqi` | Índice de qualidade do ar simulado | `index` |
| `wind_speed` | Velocidade do vento | `m/s` |
| `wind_direction` | Direção do vento | `deg` |
| `atmospheric_pressure` | Pressão atmosférica | `hPa` |
| `rainfall` | Intensidade da chuva | `mm/h` |
| `noise_level` | Ruído ambiente | `dB` |
| `visibility` | Visibilidade | `km` |
| `uv_index` | Índice UV | `index` |

## Lua — postes

Serviço `sensor_posto`; 9 dispositivos padrão com IDs `poste_*`.

| Nome no protocolo | Informação | Unidade |
|---|---|---|
| `luminosity` | Luminosidade do poste | `%` |
| `power_consumption` | Potência elétrica | `W` |
| `energy_consumption` | Energia acumulada | `kWh` |
| `voltage` | Tensão elétrica | `V` |
| `current` | Corrente elétrica | `A` |
| `led_temperature` | Temperatura do LED | `C` |
| `ambient_light` | Luz ambiente | `lux` |
| `dimming_level` | Intensidade da iluminação | `%` |

## Java — semáforos

Serviço `sensor_java`; 9 dispositivos padrão com IDs `semaforo_*`.

| Nome no protocolo | Informação | Unidade |
|---|---|---|
| `state` | Fase do semáforo | `code` |
| `queue_length` | Fila veicular | `vehicles` |
| `average_wait` | Espera média no semáforo | `seconds` |
| `average_speed` | Velocidade média dos veículos | `km/h` |
| `road_occupancy` | Ocupação da via | `%` |
| `cycle_duration` | Duração do ciclo semafórico | `seconds` |
| `pedestrians_count` | Fluxo de pedestres | `pedestrians/min` |
| `green_remaining` | Tempo restante da fase verde | `seconds` |

## Python — câmeras

Serviço `sensor_camera`; 9 dispositivos padrão com IDs `camera_*`.

| Nome no protocolo | Informação | Unidade |
|---|---|---|
| `vehicles_count` | Fluxo de veículos | `veh/min` |
| `infractions` | Infrações detectadas na leitura | `count` |
| `average_speed` | Velocidade média dos veículos | `km/h` |
| `road_occupancy` | Ocupação da via | `%` |
| `accidents` | Acidentes detectados na leitura | `count` |
| `pedestrians_count` | Fluxo de pedestres | `pedestrians/min` |
| `detection_confidence` | Confiança da detecção | `%` |
| `heavy_vehicles_count` | Fluxo de veículos pesados | `veh/min` |

## Go — estacionamentos

Serviço `sensor_estacionamento`; 9 dispositivos padrão com IDs `parking_*`.

| Nome no protocolo | Informação | Unidade |
|---|---|---|
| `total_spaces` | Total de vagas | `spaces` |
| `occupied_spaces` | Vagas ocupadas | `spaces` |
| `available_spaces` | Vagas disponíveis | `spaces` |
| `occupancy_rate` | Ocupação do estacionamento | `%` |
| `vehicle_turnover` | Taxa suavizada de rotatividade de veículos | `vehicles/min` |
| `arrivals` | Chegadas no intervalo | `vehicles` |
| `departures` | Saídas no intervalo | `vehicles` |
| `parking_wait_time` | Espera no estacionamento | `min` |
| `hourly_revenue` | Receita estimada por hora | `BRL/h` |
| `ev_charging_vehicles` | Veículos elétricos em recarga | `vehicles` |
| `ev_charging_power` | Potência da recarga elétrica | `kW` |

## Rust — água e saneamento

Serviço `sensor_agua`; 9 dispositivos padrão com IDs `water_*`.

| Nome no protocolo | Informação | Unidade |
|---|---|---|
| `water_level` | Nível da água | `%` |
| `water_flow` | Vazão da água | `L/min` |
| `water_pressure` | Pressão da água | `bar` |
| `water_ph` | pH da água | `pH` |
| `water_turbidity` | Turbidez da água | `NTU` |
| `water_temperature` | Temperatura da água | `°C` |
| `conductivity` | Condutividade da água | `uS/cm` |
| `leak_rate` | Vazamento estimado | `L/min` |

## TypeScript — lixeiras

Serviço `sensor_lixeiras`; 9 dispositivos padrão com IDs `waste_*`.

| Nome no protocolo | Informação | Unidade |
|---|---|---|
| `waste_fill_level` | Ocupação da lixeira | `%` |
| `waste_weight` | Peso dos resíduos | `kg` |
| `bin_temperature` | Temperatura da lixeira | `C` |
| `bin_humidity` | Umidade da lixeira | `%` |
| `battery_level` | Carga da bateria | `%` |
| `signal_strength` | Intensidade do sinal | `dBm` |
| `collection_count` | Coletas realizadas | `count` |
| `tilt_angle` | Inclinação da lixeira | `deg` |
| `fire_risk` | Indicador simulado de aquecimento | `%` |

## Interpretação dos valores

- `state` representa vermelho = 1, amarelo = 2 e verde = 3. A média desse código
  é apenas uma operação numérica de demonstração; inspecione a série para observar
  as transições de fase.
- `energy_consumption` e `collection_count` são acumuladores por dispositivo do
  processo do sensor e recomeçam quando ele reinicia. A energia integra a última
  potência medida enquanto o poste está ligado; o contador de coletas aumenta
  quando a lixeira chega a 95% de ocupação e é esvaziada pela simulação.
- `arrivals` e `departures` contam eventos do intervalo de leitura, enquanto
  `vehicle_turnover` é uma taxa suavizada em veículos por minuto, calculada a
  partir dos fluxos e da frequência de leitura configurada.
- `rainfall` é intensidade em milímetros por hora; `hourly_revenue` é uma estimativa
  por hora, não um saldo de receita acumulada.
- `fire_risk`, AQI e `leak_rate` são indicadores da simulação. O dashboard não
  atribui faixas de segurança inventadas às métricas novas.

As quantidades podem ser alteradas no [`.env.example`](../projeto-sockets/.env.example).
As tabelas acima descrevem a configuração padrão; não representam a quantidade de
leituras por segundo. Cada sensor possui seu próprio intervalo de publicação.
