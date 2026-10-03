# Protocolo de comunicação

[Documentação](README.md) · [Arquitetura](architecture.md) · [Operações](operations.md)

## Contrato canônico

Todos os processos usam **Protocol Buffers 3**. O schema canônico é
[`common/messages.proto`](../projeto-sockets/common/messages.proto); bindings de
C, Lua, Java, Python e Go são gerados durante os respectivos builds.

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

Estados de dispositivo são `STATUS_ON` (`1`), `STATUS_OFF` (`2`) e
`STATUS_ERROR` (`3`). Comandos de atuação aceitam somente `ON` e `OFF`;
`STATUS_ERROR` é informativo e não é um alvo válido.

Requisições do cliente podem listar dispositivos, enviar comandos ou realizar
consultas analíticas. As operações analíticas são média, desvio-padrão amostral
e maior variação.

## Canais e portas

| Origem → destino | Porta | Transporte | Conteúdo |
|---|---:|---|---|
| Sensor → gateway | 5000 | UDP | `DataPayload` |
| Sensor → gateway | 5002 | UDP | `DiscoveryResponse` |
| Gateway → sensores | 5005 | UDP multicast | Probe literal `SMARTCITY_DISCOVERY_PROBE` |
| Dashboard → gateway | 5001 | TCP | `ClientRequest` / `ClientResponse` |
| Gateway → semáforo | 5003 | TCP | `ConfigCommand` / `ConfigResponse` |
| Gateway → câmera | 5004 | TCP | `ConfigCommand` / `ConfigResponse` |
| Gateway → poste | 5006 | TCP | `ConfigCommand` / `ConfigResponse` |
| Gateway → estacionamento Go | 5007 | TCP | `ConfigCommand` / `ConfigResponse` |

O sensor climático não expõe servidor de controle. `5005/UDP` é usado somente
na rede do laboratório e não precisa ser publicado no host.

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

O limite padrão é 1 MiB (`TCP_MAX_FRAME_BYTES` no gateway e limite equivalente
nos sensores). Leituras parciais, timeout, EOF e Protobuf inválido encerram a
requisição sem aplicar uma alteração.

## Mensagens

### `DiscoveryResponse`

Anuncia identidade e capacidade de controle. Contém `message_id`, `timestamp`,
`device_id`, tipo, endereço anunciado, porta de controle, estado inicial e
`is_controllable`.

O gateway usa o **IP de origem observado no datagrama** para alcançar o sensor;
o endereço declarado é apenas informação diagnóstica. Um dispositivo
controlável precisa anunciar uma porta entre 1 e 65535. Um dispositivo não
controlável deve usar porta zero.

### `DataPayload`

Transporta `message_id`, instante da amostra, dispositivo, estado corrente e uma
lista de `Metric { name, value, unit }`. Um dispositivo desligado pode enviar um
payload de presença sem métricas; um valor de métrica deve ser finito.

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

O gateway compara o `command_id` da confirmação com o comando enviado e valida
seu timestamp. Em confirmações de sucesso, aceita apenas estados válidos e
frequência de 1 a 60; os campos solicitados precisam corresponder aos valores
pedidos. Um comando que altera somente frequência pode manter `STATUS_ERROR`
como estado informativo.

Os sensores controláveis validam esses campos novamente na fronteira local. O
sensor Go acrescenta formato restrito para IDs, rejeita comandos com mais de
cinco minutos, tolera no máximo um minuto de relógio futuro e mantém uma janela
de dez minutos para rejeitar replay do mesmo `command_id`.

### `ClientRequest` e `ClientResponse`

O envelope do dashboard possui `message_id`, `timestamp` e `type`. Conforme o
tipo, inclui comando ou parâmetros analíticos. A resposta correlaciona a
requisição por `message_id` e pode transportar inventário, resultado escalar,
metadados e pontos de gráfico.

Para uma requisição Protobuf válida, inclusive quando rejeitada por validação,
o gateway devolve `ClientResponse.message_id = ClientRequest.message_id`. O
cliente verifica essa igualdade antes de usar a resposta. Erros de framing ou
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

As portas do laboratório não são autenticadas. O gateway, portanto, trata todo
payload como não confiável e aplica os seguintes limites antes de alocar trabalho
assíncrono ou acessar o banco:

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

Com `STATUS_OFF`, o dispositivo mantém seu estado interno e deixa de publicar as
métricas operacionais. Ao retornar para `STATUS_ON`, a simulação continua. A
frequência é individual por dispositivo e pode ser alterada entre 1 e 60
segundos sem afetar os demais IDs do mesmo processo.

## Evolução compatível

Ao alterar o protocolo:

1. adicione campos com números novos;
2. reserve números removidos em vez de reutilizá-los;
3. prefira valores novos de enum ao reinterpretar valores existentes;
4. regenere todos os bindings no build;
5. execute testes Python e Go e o build completo do Compose;
6. atualize este documento, o dashboard e os validadores do gateway.
