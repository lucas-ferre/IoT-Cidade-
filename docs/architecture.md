# Arquitetura

[Documentação](README.md) · [Protocolo](protocol.md) · [Operações](operations.md)

## Visão geral

O sistema simula uma infraestrutura urbana distribuída com sete linguagens de
sensores e dois hubs de entrada. Os hubs interceptam descoberta, telemetria,
consultas e comandos antes de acessar o gateway, que centraliza SQLite e OLAP.
A disposição vertical abaixo mantém a topologia legível no GitHub:

```text
Sensores (7 linguagens)
    | UDP 5000 / 5002
    v
hub_sensores
    |
    v
gateway <--> SQLite
    ^
    | TCP 5001
hub_acesso
    ^
    | TCP 5001
dashboard (HTTP 8501)

Controle:
gateway --> hub_sensores:5010 --> sensor

Teste opt-in:
sensor_invasor --> hubs
```

`smart_city_net` contém sensores, dashboard e hubs. `gateway_backend` tem
`internal: true` e contém somente gateway e os dois hubs. O gateway não publica
portas no host; os hubs publicam UDP/5000, UDP/5002 e TCP/5001, e o dashboard
publica HTTP/8501, todos em `127.0.0.1` por padrão. As portas nativas de controle
dos sensores e TCP/5010 do hub permanecem internas.

## Componentes

| Componente | Implementação | Modelo de concorrência | Responsabilidade | Controle |
|---|---|---|---|---|
| `hub_sensores` | Python 3.11 | `asyncio`, registro e limites por origem | Autorizar perfil/origem, filtrar UDP e encaminhar comandos | 5010/TCP |
| `hub_acesso` | Python 3.11 | `asyncio`, conexões e frames limitados | Autorizar clientes, validar pedidos e correlacionar respostas | Cliente em 5001/TCP |
| `gateway` | Python 3.11 | `asyncio`, fila limitada e pool SQLite | Ingestão, descoberta, presença, proxy de comandos e OLAP | Cliente em 5001/TCP |
| `dashboard` | Python 3.11 + Streamlit | Sessão web e cliente TCP persistente | Inventário, atuação, gráficos e inspeção | — |
| `sensor_clima` | C11/POSIX | `pthreads` | Clima e qualidade do ar | Não controlável |
| `sensor_posto` | Lua 5.4 | Agendamento cooperativo | Iluminação e consumo elétrico | 5006/TCP |
| `sensor_java` | Java 21 | Threads e interrupção cooperativa | Estado e fila de tráfego | 5003/TCP |
| `sensor_camera` | Python 3.11 | `threading` e evento de encerramento | Fluxo e infrações de trânsito | 5004/TCP |
| `sensor_estacionamento` | Go 1.23 | Goroutines, `context` e estado protegido por mutex | Ocupação e rotatividade de vagas | 5007/TCP |
| `sensor_agua` | Rust 1.90 | Threads, estado protegido e encerramento por sinal | Distribuição e qualidade simulada da água | 5008/TCP |
| `sensor_lixeiras` | TypeScript / Node 22.18+ | Event loop e UDP assíncrono | Ocupação, peso, bateria e estado de lixeiras | Não controlável |
| `sensor_invasor` | Python 3.11 | Emissão sequencial e finita | Cenários inválidos para testar a interceptação | Não controlável |

Todos usam o contrato em
[`common/messages.proto`](../projeto-sockets/common/messages.proto). Rust gera
bindings com `prost` no build; TypeScript usa um codec de saída sem dependências
npm, verificado contra bindings Python gerados desse schema.

A configuração padrão representa **66 dispositivos**: 12 estações C e nove
dispositivos em cada uma das outras seis famílias. Cada amostra ligada publica
13 métricas em C; 8 em Lua, Java, Python e Rust; 11 em Go; e 9 em TypeScript.
Veja o [catálogo de métricas](metrics.md), cuja fonte no dashboard é
[`client/metric_catalog.py`](../projeto-sockets/client/metric_catalog.py).

## Fluxos principais

### Registro e presença

1. No boot, cada sensor envia `DiscoveryResponse` para `hub_sensores:5002/UDP`.
2. O hub verifica campos, prefixo/tipo/porta e se o IPv4 observado corresponde
   ao serviço DNS autorizado. Registra o endpoint real e encaminha ao gateway.
3. O gateway observa o IP do hub, faz UPSERT no SQLite e armazena 5010 como porta
   de controle dos dispositivos controláveis. Heartbeats renovam os registros.
4. O gateway marca como `STATUS_OFF` dispositivos cujo `last_seen` exceda o
   limite configurado.
5. Após reiniciar gateway ou hub, os próximos heartbeats recuperam a presença.
   O probe `239.0.0.1:5005/UDP` do gateway fica na rede backend e não atravessa
   automaticamente para a frontend; essa recuperação não depende dele.

IDs de dispositivo são estáveis. Reiniciar um container atualiza o registro
existente, em vez de gerar uma nova identidade.

### Telemetria

1. Um dispositivo produz um `DataPayload` serializado em Protobuf.
2. O sensor transmite o datagrama para `hub_sensores:5000/UDP`.
3. O hub aplica validação, limites de taxa e autorização de origem, e exige
   descoberta vigente. O gateway valida novamente antes de persistir.
4. Mensagens duplicadas ou atrasadas são descartadas.
5. Payloads válidos entram em uma fila assíncrona limitada.
6. O consumidor persiste lotes e atualiza os rollups no mesmo ciclo de escrita.

Quando a fila está cheia, o gateway descarta o novo datagrama e contabiliza o
evento; ele não cria uma quantidade ilimitada de tasks aguardando espaço. Essa
backpressure mantém o consumo de memória previsível sob rajadas.

### Controle remoto

1. O dashboard envia `ClientRequest` de tipo `SEND_COMMAND` ao `hub_acesso`.
2. O hub autoriza a origem `dashboard`, valida o pedido e o encaminha ao gateway.
3. O gateway valida alvo/comando e conecta ao IP observado do `hub_sensores`,
   porta 5010. O hub procura o alvo em seu registro ainda vigente.
4. O hub conecta ao endpoint original do sensor: 5003, 5004, 5006, 5007 ou 5008.
   IDs desconhecidos não provocam conexões a endereços declarados no pacote.
5. O sensor aplica a alteração individual e responde com `ConfigResponse`.
   Hub e gateway conferem o ID do comando e os valores efetivamente aplicados.
6. O `hub_acesso` confere a correlação da resposta antes de devolvê-la ao dashboard.

Os comandos disponíveis alteram somente o estado (`STATUS_ON` ou `STATUS_OFF`)
e/ou a frequência de telemetria, limitada ao intervalo de 1 a 60 segundos. O
sensor climático em C e as lixeiras TypeScript não participam desse fluxo.

### Consulta analítica

1. O dashboard envia métrica, operação, janela e alvo ao `hub_acesso`, que valida
   o pedido antes de encaminhá-lo ao gateway.
2. O gateway limita a janela e escolhe a fonte de menor granularidade adequada.
3. A agregação é calculada em SQLite e a série de gráfico é amostrada quando
   necessário.
4. O resultado escalar, os metadados e até um número limitado de pontos retornam
   em `ClientResponse`.

As operações suportadas são média, desvio-padrão amostral e maior variação. A
seleção padrão de fonte é:

| Janela | Fonte |
|---|---|
| Até 1 hora | `metrics` (eventos brutos) |
| Até 24 horas | `metrics_rollup_1m` |
| Até 7 dias | `metrics_rollup_5m` |
| Acima de 7 dias | `metrics_rollup_1h` |

A janela total aceita é limitada a 30 dias e a resposta gráfica a 2.000 pontos
por padrão. Os limites são configuráveis e estão descritos em
[Operações](operations.md#consultas-e-retenção).

## Concorrência e isolamento

O projeto usa uma estratégia adequada a cada runtime, mas preserva as mesmas
responsabilidades lógicas:

- **gateway:** protocolos UDP não bloqueantes, servidor TCP assíncrono, tasks de
  descoberta limitadas, um consumidor de telemetria em lote e pool de conexões;
- **C:** threads separam telemetria e listener multicast; heartbeats são
  agendados no ciclo principal;
- **Lua:** um loop cooperativo agenda I/O e retransmissões sem bloquear o ciclo
  principal;
- **Java:** loops de telemetria e controle são interrompidos durante o shutdown;
- **Python:** threads coordenadas por um evento compartilham o estado protegido;
- **Go:** goroutines de telemetria, heartbeat, multicast e controle são
  canceladas por `context`; mutexes protegem frota e comandos recentes;
- **Rust:** threads usam estado e replay protegidos, limites de conexão e prazo;
- **TypeScript:** timers e sockets UDP compartilham um event loop;
- **hubs:** validação UDP sem tasks por datagrama, cache DNS renovado e limites
  de registro, frames, taxa, concorrência e tempo de leitura.

Cada processo sensor pode representar vários dispositivos virtuais. O estado e
a frequência são mantidos por `device_id`, de modo que um comando não altera os
demais dispositivos do mesmo container.

## Persistência

O gateway usa SQLite em modo WAL e mantém quatro níveis de dados:

| Tabela | Granularidade | Finalidade |
|---|---:|---|
| `metrics` | Evento | Inspeção e janelas curtas |
| `metrics_rollup_1m` | 60 s | Consultas intermediárias |
| `metrics_rollup_5m` | 300 s | Consultas longas |
| `metrics_rollup_1h` | 3.600 s | Histórico consolidado |

Cada rollup armazena contagem, soma, soma dos quadrados, mínimo e máximo. Isso
permite combinar buckets e calcular estatísticas sem materializar toda a série
bruta. As funções determinísticas de bucketização, desvio-padrão, seleção de
fonte e amostragem estão isoladas em
[`gateway/analytics.py`](../projeto-sockets/gateway/analytics.py) e possuem testes
unitários.

O volume `gateway_db` preserva o banco entre reinicializações normais do Compose.
A remoção explícita de volumes elimina esse histórico.

O ledger `telemetry_messages` e o checkpoint `telemetry_state` são confirmados
na mesma transação que métricas e rollups. Uma retransmissão já confirmada não
volta a produzir efeitos depois de reiniciar o processo. Falhas de escrita
mantêm o lote para nova tentativa; cancelamentos revertem a transação antes de
devolver a conexão ao pool.

## Resiliência

- retry UDP/DNS com backoff exponencial e jitter nos sensores;
- jitter de telemetria para evitar rajadas sincronizadas;
- heartbeat de descoberta após reinício de gateway ou hubs;
- fila e concorrência limitadas nas fronteiras de entrada;
- timeouts e tamanho máximo para frames TCP;
- healthchecks do gateway, dashboard e sensores controláveis;
- encerramento que interrompe entradas e tenta drenar a telemetria por até
  20 segundos; o Compose concede 45 segundos ao gateway;
- SQLite WAL, pool de conexões, checkpoint e políticas de retenção.

UDP continua sendo um transporte sem confirmação de entrega. Os mecanismos
reduzem perda e indisponibilidade, mas não oferecem entrega exatamente uma vez.

## Fronteiras de confiança

Os hubs autorizam perfis e origens com base em serviço DNS e IPv4 observado,
isolando o gateway dos participantes da frontend. Essa identidade de rede não
é uma credencial nem uma assinatura: o protocolo continua sem TLS, DTLS ou
autenticação criptográfica. O laboratório usa IPv4 e não oferece isolamento
contra um host Docker comprometido. Consulte a
[política de segurança](../SECURITY.md).

O invasor permanece fora da execução padrão. O perfil `security-test` emite
12 cenários limitados e termina; bloqueio UDP é comprovado por auditoria e
testes de integração, pois o envio UDP sozinho não oferece confirmação.
