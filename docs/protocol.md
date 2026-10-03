# Protocolo de comunicação

[Documentação](README.md) · [Arquitetura](architecture.md) · [Operações](operations.md)

## Contrato canônico

Todos os processos usam **Protocol Buffers 3**. O schema canônico é
[`common/messages.proto`](../projeto-sockets/common/messages.proto); bindings de
C, Lua, Java, Python, Go e Rust são gerados durante os respectivos builds.
Rust usa `prost` e `protoc` empacotado nas dependências travadas. TypeScript
codifica somente mensagens de saída com um codec sem dependências npm; testes
decodificam seus bytes com bindings Python gerados do contrato compartilhado.

Campos existentes não devem ser renumerados nem reutilizados. Uma alteração no
schema precisa preservar números antigos, regenerar os bindings e ser validada
em todos os runtimes.

### Enumerações

| Enum | Valor | Significado |
|---|---:|---|
| `DEVICE_TYPE_TRAFFIC_LIGHT` | 1 | Semáforo |
| `DEVICE_TYPE_LAMP_POST` | 2 | Poste inteligente |
| `DEVICE_TYPE_WEATHER_STATION` | 3 | Estação meteorológica |
| `DEVICE_TYPE_CAMERA` | 4 | Câmera de tráfego |
| `DEVICE_TYPE_AIR_QUALITY` | 5 | Qualidade do ar |
| `DEVICE_TYPE_PARKING_SENSOR` | 6 | Estacionamento inteligente em Go |
| `DEVICE_TYPE_WATER_SENSOR` | 7 | Água em Rust |
| `DEVICE_TYPE_WASTE_SENSOR` | 8 | Lixeiras em TypeScript |

Estados de dispositivo são `STATUS_ON` (`1`), `STATUS_OFF` (`2`) e
`STATUS_ERROR` (`3`). Comandos de atuação aceitam somente `ON` e `OFF`;
`STATUS_ERROR` é informativo e não é um alvo válido.

Requisições do cliente podem listar dispositivos, enviar comandos ou realizar
consultas analíticas. As operações analíticas são média, desvio-padrão amostral
e maior variação.

## Canais e portas

| Origem → destino | Porta | Transporte | Conteúdo |
|---|---:|---|---|
| Sensor → hub_sensores → gateway | 5000 | UDP | `DataPayload` |
| Sensor → hub_sensores → gateway | 5002 | UDP | `DiscoveryResponse` |
| Dashboard → hub_acesso → gateway | 5001 | TCP | `ClientRequest` / `ClientResponse` |
| Gateway → hub_sensores | 5010 | TCP | `ConfigCommand` / `ConfigResponse` |
| Hub_sensores → semáforo | 5003 | TCP | `ConfigCommand` / `ConfigResponse` |
| Hub_sensores → câmera | 5004 | TCP | `ConfigCommand` / `ConfigResponse` |
| Hub_sensores → poste | 5006 | TCP | `ConfigCommand` / `ConfigResponse` |
| Hub_sensores → estacionamento Go | 5007 | TCP | `ConfigCommand` / `ConfigResponse` |
| Hub_sensores → água Rust | 5008 | TCP | `ConfigCommand` / `ConfigResponse` |
| Gateway → backend | 5005 | UDP multicast | Probe literal `SMARTCITY_DISCOVERY_PROBE` |

Clima e lixeiras não expõem controle. Os hubs pertencem às redes frontend e
backend; o gateway pertence somente à backend interna. Os probes do gateway
não atravessam automaticamente para os sensores na frontend. Heartbeats
periódicos renovam a descoberta e recuperam reinicializações.

O host publica somente UDP/5000 e UDP/5002 do `hub_sensores`, TCP/5001 do
`hub_acesso` e HTTP/8501 do dashboard. A publicação usa loopback por padrão e
não dispensa a autorização de origem aplicada pelo hub de acesso.

## Framing TCP

TCP é um fluxo de bytes e não preserva fronteiras de mensagem. Por isso, toda
mensagem TCP é precedida por um comprimento sem sinal de quatro bytes em
**big-endian**:

```text
offset  tamanho  conteúdo
0       4        N, uint32 big-endian
4       N        mensagem Protobuf serializada
```

O leitor deve:

1. ler exatamente quatro bytes;
2. interpretar `N` como inteiro `uint32` big-endian;
3. rejeitar `N == 0` ou tamanho superior ao limite;
4. ler exatamente `N` bytes;
5. desserializar a mensagem esperada para aquele canal.

O limite padrão é 1 MiB (`TCP_MAX_FRAME_BYTES` no gateway,
`HUB_TCP_MAX_FRAME_BYTES` nos hubs e limite equivalente nos sensores). Leituras
parciais, timeout, EOF e Protobuf inválido encerram a
requisição sem aplicar uma alteração.

## Mensagens

### `DiscoveryResponse`

Anuncia identidade e capacidade de controle. Contém `message_id`, `timestamp`,
`device_id`, tipo, endereço anunciado, porta de controle, estado inicial e
`is_controllable`.

O hub verifica o **IP observado no datagrama** contra o DNS do serviço
autorizado e exige o tipo, prefixo e porta nativa daquele perfil. Armazena esse
endpoint real, transforma `ip_address` em informação sobre a origem e anuncia
`control_port=5010` para sensores controláveis. O gateway observa o IP do hub
e envia controle a ele. O hub resolve o alvo pelo seu registro, nunca pelo
endereço que o sensor declarou. Sensores sem controle usam porta zero.

| Prefixo | Serviço DNS | Tipo | Porta nativa |
|---|---|---:|---:|
| `estacao_` | `sensor_clima` | 3 | 0 |
| `poste_` | `sensor_posto` | 2 | 5006 |
| `semaforo_` | `sensor_java` | 1 | 5003 |
| `camera_` | `sensor_camera` | 4 | 5004 |
| `parking_` | `sensor_estacionamento` | 6 | 5007 |
| `water_` | `sensor_agua` | 7 | 5008 |
| `waste_` | `sensor_lixeiras` | 8 | 0 |

O registro do hub expira 120 segundos após a última descoberta aceita por
padrão. Telemetria anterior à descoberta ou com registro expirado é descartada.

### `DataPayload`

Transporta `message_id`, instante da amostra, dispositivo, estado corrente e uma
lista de `Metric { name, value, unit }`. Um dispositivo desligado pode enviar um
payload de presença sem métricas; um valor de métrica deve ser finito. O hub
também rejeita nomes de métrica duplicados dentro do mesmo payload. Consulte
o [catálogo de métricas](metrics.md) para nomes e unidades de cada família.

### `ConfigCommand` e `ConfigResponse`

Um comando identifica de forma explícita:

- `command_id` único e não vazio;
- `timestamp` Unix positivo;
- `target_device_id` não vazio;
- ao menos uma alteração: `update_status` e/ou `update_frequency`;
- `target_status` igual a `STATUS_ON` ou `STATUS_OFF`, quando solicitado;
- `new_frequency_secs` entre 1 e 60, quando solicitado.

O `target_device_id` interno deve coincidir com o alvo do `ClientRequest`. O
sensor responde com sucesso, mensagem textual, estado e frequência efetivamente
aplicados. Rejeições também retornam `ConfigResponse` e não modificam o estado.

O hub de sensores e o gateway comparam o `command_id` da confirmação com o
comando enviado e validam
seu timestamp. Em confirmações de sucesso, aceita apenas estados válidos e
frequência de 1 a 60; os campos solicitados precisam corresponder aos valores
pedidos. Um comando que altera somente frequência pode manter `STATUS_ERROR`
como estado informativo.

Os sensores controláveis validam esses campos novamente na fronteira local e
mantêm proteção contra repetição de comandos. O hub impõe no máximo cinco
minutos de idade e 30 segundos de relógio futuro, mesmo quando o runtime do
sensor aceita uma tolerância maior. A proteção local de Go e Rust mantém IDs
recentes por dez minutos. Um ID desconhecido ou registro expirado no hub não
abre conexão de controle.

### `ClientRequest` e `ClientResponse`

O envelope do dashboard possui `message_id`, `timestamp` e `type`. Conforme o
tipo, inclui comando ou parâmetros analíticos. A resposta correlaciona a
requisição por `message_id` e pode transportar inventário, resultado escalar,
metadados e pontos de gráfico.

Para uma requisição Protobuf válida, inclusive quando rejeitada por validação,
o gateway devolve `ClientResponse.message_id = ClientRequest.message_id`. O
hub de acesso e o cliente verificam essa igualdade antes de usar a resposta.
Uma rejeição de pedido válido no hub também preserva seu ID. Erros de framing ou
Protobuf que impedem identificar a requisição não possuem essa correlação.

Uma consulta analítica requer:

- `query_metric` não vazia;
- operação conhecida;
- timestamps inicial e final positivos e ordenados;
- janela dentro de `OLAP_MAX_QUERY_WINDOW_SECS`;
- `target_device_id` opcional.

A fonte OLAP é a menor granularidade permitida pela duração que também cubra a
idade do início da janela conforme a retenção: bruta, 1 minuto, 5 minutos ou
1 hora. Retenção zero é ilimitada. Rollups consultam buckets completos; as bordas
podem abranger instantes fora do período pedido. Os metadados explicitam a
granularidade e a janela agregada. A política de retenção indica cobertura
possível, sem garantir que houve coleta naquele período.

## Validação na entrada

Os hubs aplicam autorização de origem por DNS/IPv4, perfil e limites de taxa e
concorrência. Isso não autentica autoria por credenciais ou criptografia. O
gateway revalida os envelopes antes de alocar trabalho ou acessar o banco:

| Regra | Padrão |
|---|---:|
| Datagramas UDP | Máximo de 16 KiB |
| `device_id`, `message_id` e `command_id` | Obrigatórios; máximo de 128 caracteres |
| Nome de métrica | Obrigatório; máximo de 128 caracteres |
| Unidade | Máximo de 32 caracteres |
| Métricas por `DataPayload` | Máximo de 64 |
| Valores | Apenas números finitos; `NaN` e infinito são rejeitados |
| Idade de mensagens | Máximo de 24 horas |
| Tolerância de timestamp futuro | 300 segundos |
| Janela OLAP | Máximo de 30 dias |
| Pontos no gráfico | Máximo de 2.000 |

O hub de acesso autoriza apenas `dashboard` por padrão. `ACCESS_ALLOWED_HOSTS`
permite uma lista explícita de serviços ou IPv4 adicionais. O hub de sensores
aceita comandos somente do IPv4 associado ao `GATEWAY_HOST` configurado. Os
nomes DNS consultados vêm da configuração, nunca de um endereço recebido no
payload. O transporte e essas listas usam IPv4.

Campos textuais não podem começar/terminar com espaços nem conter caracteres de
controle. Tipos, estados, operações e portas precisam pertencer aos domínios
válidos. Os limites são configuráveis; veja
[Operações](operations.md#limites-de-entrada).

## Idempotência e ordem

Cada telemetria possui `message_id`. O SQLite mantém um ledger
`telemetry_messages` com chave `(device_id, message_id)` e o checkpoint de ordem
`telemetry_state` por dispositivo. Ambos são confirmados na mesma transação que
as métricas brutas, rollups e atualização de presença. O gateway descarta:

- duplicatas com a mesma identidade lógica de mensagem;
- payloads cujo timestamp seja anterior ao último confirmado para o dispositivo;
- mensagens fora da janela de idade ou excessivamente futuras.

IDs distintos no mesmo segundo são aceitos, mas a retransmissão de um ID já
confirmado não produz novos efeitos, mesmo após reiniciar o gateway. Falhas de
escrita não confirmam a identidade e o worker repete o lote. O ledger expira IDs
somente quando `MESSAGE_MAX_AGE_SECS > 0` e a mensagem já seria antiga demais;
com valor zero, esses IDs permanecem. O checkpoint de ordem não é apagado por
essa limpeza.

UDP não confirma entrega, e sobrecarga ou encerramento sem concluir a drenagem
ainda podem perder payloads. O relógio dos containers precisa estar razoavelmente
sincronizado. Veja os prazos de encerramento em [Operações](operations.md).

## Sensor de estacionamento em Go

O sensor Go representa áreas agregadas de estacionamento. Cada processo simula
uma frota de dispositivos com IDs estáveis no formato `parking_<setor>_<nn>` e
tipo `DEVICE_TYPE_PARKING_SENSOR` (`6`). Ele anuncia controle em 5007/TCP.

| Métrica | Unidade | Invariante |
|---|---|---|
| `total_spaces` | `spaces` | Capacidade positiva do setor |
| `occupied_spaces` | `spaces` | Entre zero e a capacidade |
| `available_spaces` | `spaces` | `total_spaces - occupied_spaces` |
| `occupancy_rate` | `%` | `100 × occupied_spaces / total_spaces` |
| `vehicle_turnover` | `vehicles/min` | Taxa simulada não negativa e suavizada |
| `arrivals` / `departures` | `vehicles` | Movimentação não negativa no intervalo |
| `parking_wait_time` | `min` | Espera simulada não negativa |
| `hourly_revenue` | `BRL/h` | Receita estimada não negativa |
| `ev_charging_vehicles` | `vehicles` | Entre zero e as vagas ocupadas |
| `ev_charging_power` | `kW` | Compatível com os veículos em recarga |

`parking_wait_time` usa minutos. O semáforo mantém `average_wait` em segundos;
os nomes diferentes evitam misturar unidades em consultas entre famílias.

Com `STATUS_OFF`, o dispositivo mantém seu estado interno e deixa de publicar as
métricas operacionais. Ao retornar para `STATUS_ON`, a simulação continua. A
frequência é individual por dispositivo e pode ser alterada entre 1 e 60
segundos sem afetar os demais IDs do mesmo processo.

## Novas famílias

Rust anuncia `water_<setor>_<nn>`, tipo 7, com oito métricas de água e controle
em 5008/TCP. Seus bindings são gerados pelo `build.rs` a partir do schema
compartilhado, com versões fixadas no `Cargo.lock` e builds `--locked`.

TypeScript anuncia `waste_<setor>_<nn>`, tipo 8, com nove métricas de lixeiras e
sem controle. Node 22.18 ou superior executa os arquivos `.ts` por remoção de
tipos. Não há instalação de dependências npm. Os testes de compatibilidade
verificam enum novo, int64, fixed64, UTF-8 e ordem dos campos contra Protobuf
gerado em Python.

O emissor invasor do perfil `security-test` não integra o catálogo de sensores
autorizados. Seus 12 cenários incluem identidade desconhecida, falsificação de
câmera e envelopes/valores inválidos. A auditoria dos hubs e testes com destino
backend observado comprovam rejeição; um envio UDP isolado não comprova bloqueio.

## Evolução compatível

Ao alterar o protocolo:

1. adicione campos com números novos;
2. reserve números removidos em vez de reutilizá-los;
3. prefira valores novos de enum ao reinterpretar valores existentes;
4. regenere todos os bindings no build;
5. execute testes Python, Go, Rust e TypeScript, a verificação independente do
   codec TypeScript e o build dos sensores C, Lua e Java;
6. atualize este documento, o catálogo, o dashboard, os perfis dos hubs e os
   validadores do gateway;
7. execute o smoke test completo e os cenários invasores no Compose.
