# Operações

[Documentação](README.md) · [Arquitetura](architecture.md) · [Protocolo](protocol.md)

## Pré-requisitos

Para a execução padrão, use Docker Desktop com Compose v2 ou Podman com suporte
a Compose. As toolchains de Python, C, Lua, Java, Go, Rust, Node e Protobuf são instaladas
nas imagens; não é necessário instalá-las no host para iniciar o laboratório.

Detalhes de preparação estão em [SETUP.md](../SETUP.md).

## Iniciar e encerrar

Execute os comandos no diretório `projeto-sockets`:

```bash
cd projeto-sockets
docker compose config --quiet
docker compose up --build --wait
```

Abra `http://localhost:8501`. Para acompanhar o estado:

```bash
docker compose ps
docker compose logs -f gateway hub_sensores hub_acesso dashboard
```

Encerre preservando o banco:

```bash
docker compose down
```

Ao receber `SIGTERM` ou `SIGINT`, o gateway interrompe novas entradas UDP/TCP,
encerra os trabalhos de rede e tenta gravar a telemetria já admitida antes de
fechar o SQLite. `TELEMETRY_SHUTDOWN_TIMEOUT_SECS` limita a espera pela drenagem
a 20 segundos por padrão; o Compose concede `stop_grace_period: 45s` ao gateway.
Se o prazo terminar ou o banco não recuperar a escrita, os logs registram erro
e os pacotes sem confirmação podem ser perdidos. A fila permanece em memória.

`docker compose down --volumes` também remove o volume `gateway_db` e todo o
histórico SQLite. Use essa opção somente quando a perda do histórico for
intencional.

No Podman, substitua `docker compose` por `podman compose` ou use o helper
[`scripts/podman-compose.ps1`](../projeto-sockets/scripts/podman-compose.ps1).

## Serviços e portas

### Publicadas no host

| Serviço | Bind padrão | Transporte | Uso |
|---|---|---|---|
| `hub_sensores` | `127.0.0.1:5000` | UDP | Telemetria |
| `hub_acesso` | `127.0.0.1:5001` | TCP | Cliente/dashboard autorizado |
| `hub_sensores` | `127.0.0.1:5002` | UDP | Descoberta |
| `dashboard` | `127.0.0.1:8501` | HTTP/TCP | Interface Streamlit |

### Somente na rede Compose

| Serviço | Porta | Transporte | Uso |
|---|---:|---|---|
| `gateway` | 5000 / 5001 / 5002 | UDP / TCP / UDP | Entradas somente na backend interna |
| `hub_sensores` | 5010 | TCP | Controle recebido do gateway |
| `sensor_java` | 5003 | TCP | Controle Java |
| `sensor_camera` | 5004 | TCP | Controle Python |
| Multicast | 5005 | UDP | Probe restrito à rede de emissão |
| `sensor_posto` | 5006 | TCP | Controle Lua |
| `sensor_estacionamento` | 5007 | TCP | Controle Go |
| `sensor_agua` | 5008 | TCP | Controle Rust |

O gateway não publica portas. `gateway_backend` é uma bridge interna com
somente gateway e hubs. `smart_city_net` reúne hubs, dashboard e sensores. Clima
e lixeiras não têm controle TCP. Os comandos seguem gateway → hub/5010 → porta
nativa do sensor.

Não publique as portas internas. Para acesso a partir de outra máquina,
`BIND_ADDRESS=0.0.0.0` altera somente as portas explicitamente publicadas no
Compose. Essa configuração exige rede confiável, firewall e a leitura prévia de
[SECURITY.md](../SECURITY.md).

## Configuração

Copie o exemplo antes de personalizar o ambiente:

```bash
cp .env.example .env
```

O arquivo `.env` é local e não deve ser versionado.

### Topologia e frota

| Variável | Padrão | Efeito |
|---|---:|---|
| `BIND_ADDRESS` | `127.0.0.1` | Interface usada nas portas publicadas |
| `LOG_LEVEL` | `INFO` | Verbosidade do gateway |
| `C_DEVICE_COUNT` | `12` | Estações ambientais no processo C |
| `LUA_DEVICE_COUNT` | `9` | Postes no processo Lua |
| `JAVA_DEVICE_COUNT` | `9` | Semáforos no processo Java |
| `CAMERA_DEVICE_COUNT` | `9` | Câmeras no processo Python |
| `GO_PARKING_DEVICE_COUNT` | `9` | Estacionamentos no processo Go, entre 1 e 100 |
| `RUST_WATER_DEVICE_COUNT` | `9` | Pontos de água Rust, entre 1 e 100 |
| `TS_WASTE_DEVICE_COUNT` | `9` | Lixeiras TypeScript, entre 1 e 100 |

Esses padrões totalizam 66 dispositivos. As famílias publicam respectivamente
13, 8, 8, 8, 11, 8 e 9 métricas por amostra ligada. Consulte o
[catálogo](metrics.md), incluindo `parking_wait_time` em minutos, separado de
`average_wait` dos semáforos em segundos.

Não use `docker compose up --scale` para duplicar um processo sensor sem também
definir uma estratégia de IDs: réplicas com a mesma configuração anunciariam as
mesmas identidades. Para aumentar a frota simulada, prefira as variáveis de
contagem.

### Presença e recuperação

| Variável | Padrão | Efeito |
|---|---:|---|
| `SENSOR_HEARTBEAT_INTERVAL_SECS` | `10` | Intervalo base de renovação de presença |
| `SENSOR_HEARTBEAT_JITTER_SECS` | `2` | Jitter do heartbeat |
| `SENSOR_DISCOVERY_JITTER_SECS` | `2` | Jitter de resposta multicast do sensor Go |
| `DISCOVERY_PROBE_INTERVAL_SECS` | `15` | Intervalo dos probes do gateway |
| `MULTICAST_TTL` | `1` | Alcance IP do probe multicast |
| `DEVICE_OFFLINE_TIMEOUT_SECS` | `45` | Ausência antes de marcar o dispositivo offline |
| `DEVICE_OFFLINE_CHECK_INTERVAL_SECS` | `5` | Intervalo da varredura de presença |
| `DISCOVERY_MAX_IN_FLIGHT` | `256` | Processamentos de descoberta concorrentes |

O Compose define `GATEWAY_HOST=hub_sensores` nos sensores e
`GATEWAY_HOST=hub_acesso` no dashboard. Os hubs usam `GATEWAY_HOST=gateway`.
Os runtimes podem ter `gateway` como padrão quando executados isoladamente;
preserve as substituições do Compose para manter a interceptação.

O probe multicast do gateway fica na backend e não chega automaticamente à
frontend. O próximo heartbeat de descoberta recupera sensores após reiniciar
gateway ou hub. A topologia atual e os perfis de origem usam IPv4.

### Hubs e autorização de origem

| Variável | Padrão no Compose | Efeito |
|---|---:|---|
| `ACCESS_ALLOWED_HOSTS` | `dashboard` | CSV explícito de serviços/IPv4 autorizados |
| `HUB_MAX_CLIENTS` | `64` no hub de acesso | Limite de conexões simultâneas |
| `HUB_RATE_PER_SECOND` | `120` no hub de sensores | Reposição de tokens por origem/canal |
| `HUB_RATE_BURST` | `240` no hub de sensores | Capacidade de rajada por origem/canal |
| `HUB_DEVICE_TTL_SECS` | `120` | Tempo de validade após descoberta aceita |

Somente o IPv4 do serviço autorizado para cada prefixo/tipo/porta pode registrar
sensores. O hub não usa o endereço anunciado para escolher um endpoint. O
controle recebido em 5010 exige origem correspondente ao gateway. Veja os
[perfis completos](protocol.md#discoveryresponse).

Conectar a TCP/5001 publicado no host não autoriza automaticamente uma consulta:
o IP visto pelo hub precisa constar em `ACCESS_ALLOWED_HOSTS`. O endereço visto
em uma conexão externa pode ser o da bridge/NAT do runtime. Consulte
`source_ip` na auditoria e autorize explicitamente esse endereço para um cliente
local adicional; manter somente `dashboard` é suficiente para a interface web.

Outras opções do processo constam no
[README dos hubs](../projeto-sockets/hubs/README.md): timeout de 10 segundos,
frame de 1 MiB, datagrama de 16 KiB, registro de até 2.048 dispositivos e DNS
renovado a cada 15 segundos. Para alterar uma opção não repassada pelo Compose,
adicione-a ao `environment` do serviço; um valor em `.env` sozinho não injeta
uma variável ausente dessa configuração. Falhas de DNS revogam autorizações
antigas após três intervalos de renovação.

### Ingestão e SQLite

| Variável | Padrão | Efeito |
|---|---:|---|
| `DB_POOL_SIZE` | `4` | Conexões reutilizadas pelo gateway |
| `WAL_CHECKPOINT_INTERVAL_SECS` | `300` | Intervalo do checkpoint WAL |
| `TELEMETRY_QUEUE_MAXSIZE` | `10000` | Capacidade da fila de datagramas válidos |
| `TELEMETRY_BATCH_MAX_PAYLOADS` | `100` | Payloads por lote |
| `TELEMETRY_BATCH_MAX_ROWS` | `500` | Linhas de métricas por lote |
| `TELEMETRY_BATCH_FLUSH_INTERVAL_SECS` | `1.0` | Tempo máximo antes do flush |
| `TELEMETRY_SHUTDOWN_TIMEOUT_SECS` | `20` | Prazo para drenar a fila no encerramento |

Se a fila atingir a capacidade, novos datagramas são descartados e o gateway
registra a contagem. Aumentar a fila apenas posterga saturação; antes disso,
observe CPU, latência e throughput de escrita.

O gateway mantém `telemetry_messages`, um ledger com chave
`(device_id, message_id)`, e `telemetry_state`, o último timestamp confirmado por
dispositivo. Ledger, checkpoint de ordem, métricas e rollups são gravados na
mesma transação. Assim, retransmissões intercaladas e reinicializações não
duplicam mensagens já confirmadas. Em falhas de escrita, o worker mantém o lote
e repete a operação. Durante o funcionamento normal, só confirma o consumo da
fila após a persistência; o prazo de encerramento limita essa espera.

### Consultas e retenção

| Variável | Padrão | Efeito |
|---|---:|---|
| `METRICS_RAW_RETENTION_SECS` | `604800` | Retém eventos brutos por 7 dias |
| `ROLLUP_1M_RETENTION_SECS` | `2592000` | Retém rollup de 1 minuto por 30 dias |
| `ROLLUP_5M_RETENTION_SECS` | `15552000` | Retém rollup de 5 minutos por 180 dias |
| `ROLLUP_1H_RETENTION_SECS` | `31536000` | Retém rollup de 1 hora por 365 dias |
| `METRICS_RETENTION_INTERVAL_SECS` | `3600` | Intervalo de limpeza |
| `ROLLUP_BACKFILL_ON_STARTUP` | `1` | Preenche rollups antigos durante o boot |
| `OLAP_RAW_MAX_WINDOW_SECS` | `3600` | Usa dados brutos até 1 hora |
| `OLAP_1M_MAX_WINDOW_SECS` | `86400` | Usa rollup de 1 minuto até 24 horas |
| `OLAP_5M_MAX_WINDOW_SECS` | `604800` | Usa rollup de 5 minutos até 7 dias |
| `OLAP_MAX_QUERY_WINDOW_SECS` | `2592000` | Janela máxima aceita: 30 dias |
| `OLAP_MAX_GRAPH_POINTS` | `2000` | Máximo de pontos em uma resposta gráfica |

O valor `0` desativa o expurgo para as variáveis de retenção. Uma janela OLAP
superior ao limite é rejeitada antes da consulta. A fonte considera duração e
idade do início da janela: uma consulta curta de 35 dias atrás pode usar rollup
de 5 minutos porque o de 1 minuto já expirou. A cobertura de rollups considera o
início do bucket, que é o campo usado no expurgo.

Buckets nas bordas incluem o intervalo inteiro de agregação, podendo ampliar a
janela solicitada. Os metadados informam a fonte, granularidade e janela
agregada. Quando há mais pontos que o máximo, o banco aplica amostragem uniforme
determinística e preserva os extremos.

### Limites de entrada

| Variável | Padrão | Efeito |
|---|---:|---|
| `TCP_CLIENT_READ_TIMEOUT` | `10` | Timeout de uma leitura TCP iniciada |
| `TCP_CLIENT_IDLE_TIMEOUT` | `60` | Ociosidade máxima da conexão persistente |
| `TCP_MAX_FRAME_BYTES` | `1048576` | Tamanho máximo do frame TCP |
| `UDP_MAX_DATAGRAM_BYTES` | `16384` | Tamanho máximo do datagrama UDP |
| `MAX_DEVICE_ID_LENGTH` | `128` | Comprimento máximo de `device_id` |
| `MAX_MESSAGE_ID_LENGTH` | `128` | Comprimento máximo de IDs de mensagem/comando |
| `MAX_METRIC_NAME_LENGTH` | `128` | Comprimento máximo do nome da métrica |
| `MAX_METRIC_UNIT_LENGTH` | `32` | Comprimento máximo da unidade |
| `MAX_METRICS_PER_PAYLOAD` | `64` | Métricas por telemetria |
| `MESSAGE_MAX_AGE_SECS` | `86400` | Idade máxima das mensagens recebidas |
| `MESSAGE_MAX_FUTURE_SKEW_SECS` | `300` | Tolerância de relógio futuro |

Os IDs do ledger são removidos somente quando `MESSAGE_MAX_AGE_SECS > 0` e o
timestamp já saiu da janela aceita. Com `MESSAGE_MAX_AGE_SECS=0`, a limitação de
idade e o expurgo desses IDs ficam desativados. O checkpoint de ordem por
dispositivo permanece mesmo após a limpeza dos IDs.

Reduzir esses valores pode quebrar clientes legítimos. Aumentá-los amplia uso de
memória e trabalho por requisição; faça a alteração junto a testes de carga.

### Limiares dos sensores

| Serviço | Variável | Padrão | Evento |
|---|---|---:|---|
| Poste | `LUMINOSITY_LOW_THRESHOLD` | `80` | Luminosidade baixa |
| Poste | `POWER_CONSUMPTION_THRESHOLD` | `32` | Consumo elevado |
| Semáforo | `TRAFFIC_QUEUE_THRESHOLD` | `35` | Fila veicular elevada |
| Câmera | `TRAFFIC_VEHICLES_THRESHOLD` | `80` | Fluxo elevado |
| Câmera | `TRAFFIC_INFRACTIONS_THRESHOLD` | `3` | Infrações elevadas |

Frequência e estado dos sensores controláveis são parâmetros de runtime. Use o
Painel de Atuação no dashboard; a frequência permitida é de 1 a 60 segundos.

## Observabilidade

### Estado e healthchecks

```bash
docker compose ps
docker compose ps --format json
```

O gateway testa 5001/TCP; os hubs testam 5010/5001 localmente; o dashboard testa
sua porta web; sensores controláveis testam os listeners TCP; C e TypeScript
verificam o processo. Um container `healthy`
confirma disponibilidade local do processo, não a correção semântica de todas as
métricas.

### Logs

```bash
docker compose logs --since=10m gateway
docker compose logs --since=10m hub_sensores hub_acesso
docker compose logs -f sensor_estacionamento
docker compose logs --tail=200 dashboard
```

Investigue especialmente:

- mensagens rejeitadas por validação;
- descartes da fila de telemetria ou do limite de descoberta;
- timeouts ao encaminhar comandos;
- dispositivos marcados offline;
- falhas de checkpoint ou retenção SQLite;
- retries de escrita e prazo excedido ao drenar telemetria no encerramento.

Os hubs produzem JSON com `hub_id`, `event`, `reason`, `source_ip`, `device_id`
e `count`. Contadores incluem todos os pacotes; logs repetidos mostram as cinco
primeiras ocorrências e cada centésima. O evento periódico `counters` fornece
totais exatos, mesmo quando a auditoria de eventos foi amostrada.

### Banco de dados

A imagem do gateway contém Python, portanto é possível consultar SQLite sem o
executável `sqlite3`:

```bash
docker compose exec gateway python -c "import sqlite3; db=sqlite3.connect('/app/gateway/db/smartcity_gateway.db'); print(db.execute('SELECT device_id, type, status, last_seen FROM devices ORDER BY device_id').fetchall())"
```

Para uma inspeção mais longa, prefira copiar o banco e usar uma ferramenta local.
Faça isso com o gateway parado ou copie também os arquivos WAL/SHM para obter um
snapshot consistente.

## Validação e testes

### Verificações rápidas

Na raiz `projeto-sockets`, use um ambiente Python com os manifestos e
`protoc` disponível. Gere os bindings antes de executar a suíte:

```bash
docker compose config --quiet
python -m pip install -r gateway/requirements.txt -r client/requirements.txt -r sensor_python/requirements.txt -r hubs/requirements.txt -r sensor_intruder/requirements.txt
protoc -I=common --python_out=gateway common/messages.proto
protoc -I=common --python_out=client common/messages.proto
protoc -I=common --python_out=sensor_python common/messages.proto
python -m compileall -q gateway client sensor_python hubs sensor_intruder
python -m unittest discover -s tests -v
python -m unittest discover -s sensor_python -p 'test_*.py' -v
```

Os testes Python cobrem OLAP e retenção, persistência e replay, falhas/cancelamento,
drenagem, framing e correlação TCP, gráficos e classificação do dashboard, além
de validação e atuação do sensor Python. Incluem os dois hubs, o invasor e TCP/UDP
em loopback: autorização, descarte, relay, correlação e encerramento. Usam SQLite temporário e sockets locais;
não exigem os serviços Compose em execução.

### Cálculo AQI em C

Com GCC disponível, execute na raiz `projeto-sockets`:

```bash
gcc -std=c11 -Wall -Wextra -Werror sensor_c/test_aqi.c -lm -o test-aqi
./test-aqi
gcc -std=c11 -Wall -Wextra -Werror sensor_c/test_environment.c -lm -o test-environment
./test-environment
```

Os testes cobrem limites, lacunas entre faixas e monotonicidade do AQI, além de
13 métricas e relações entre chuva, umidade e UV em amostras simuladas. No Windows,
o executável pode receber o nome `test-aqi.exe` e ser executado com
`.\test-aqi.exe`.

### Sensor Go

O build da imagem gera o binding Protobuf e executa `go test ./...` antes de
produzir o binário estático:

```bash
docker compose build sensor_estacionamento
```

Os testes Go cobrem validação e replay de comandos, isolamento por dispositivo,
invariantes das métricas de estacionamento e framing TCP. Para testar com uma
toolchain Go local, gere primeiro o binding:

```bash
cd sensor_go
mkdir -p proto
protoc --proto_path=../common --go_out=./proto --go_opt=paths=source_relative ../common/messages.proto
go test ./...
```

### Rust, TypeScript, Java e Lua

Rust 1.90 ou superior gera o binding no `build.rs`, com `prost` e compilador
Protobuf empacotado. Preserve o `Cargo.lock` e use a resolução travada:

```bash
cd sensor_rust
cargo test --locked
cargo build --release --locked
```

Na raiz `projeto-sockets`, Node 22.18 ou superior executa os testes de modelo e
runtime sem instalar dependências npm. A verificação Python independente precisa
de `protoc` ou `grpcio-tools`, além de Node no PATH ou em `NODE_BINARY`:

```bash
node --experimental-strip-types --test sensor_typescript/tests/model.test.ts sensor_typescript/tests/runtime.test.ts
python -m unittest discover -s sensor_typescript/tests -p 'test_*.py' -v
javac sensor_java/TrafficSample.java sensor_java/TrafficSampleTest.java
java -cp sensor_java TrafficSampleTest
```

As suítes TypeScript contêm oito testes Node e duas verificações Python de
compatibilidade no fio. O teste Java valida as amostras sem exigir o runtime
Protobuf; o build completo compila também o sensor com o contrato compartilhado.

Com Lua 5.4 disponível, execute a partir da pasta `sensor_lua`:

```bash
lua5.4 test_lamp_metrics.lua
```

Os Dockerfiles de C, Lua, Java, Go, Rust e TypeScript executam suas verificações
durante o build. Registrar um teste no build ou no CI não demonstra que ele
passou no host atual; informe separadamente ferramentas indisponíveis e builds
que ainda precisam ser executados.

### Smoke test integrado

```bash
docker compose up --build --wait
docker compose ps
curl --fail http://localhost:8501/_stcore/health
docker compose exec dashboard python smoke.py --expected-devices 66
docker compose logs --since=2m gateway hub_sensores hub_acesso
```

O smoke test roda dentro do serviço autorizado `dashboard` e verifica
inventário, métricas e atuação pela rota dos hubs. Use 66 dispositivos com os
padrões de frota; ajuste a expectativa se alterou as contagens. A tela deve
mostrar as sete famílias, incluindo água Rust e lixeiras TypeScript.

O CI instala dependências, gera bindings, executa as suítes de linguagem,
constrói as imagens e o invasor, sobe o laboratório, roda o smoke test pelo
dashboard e emite os cenários do perfil de segurança.
Um teste unitário aprovado não confirma que o ambiente completo foi implantado.

### Teste com sensor invasor

O invasor não inicia na execução padrão. Depois de subir o laboratório:

```bash
docker compose --profile security-test run --build --rm sensor_invasor
docker compose logs --since=2m hub_sensores hub_acesso
```

Uma rodada envia nove datagramas UDP e três pedidos TCP em 12 cenários. Os
destinos são apenas os dois hubs configurados e precisam resolver para IPv4
privado ou loopback. São no máximo dez rodadas, com intervalo mínimo de 0,02 s.
`INTRUDER_ENABLED=1` fica restrito ao perfil; fora dele o emissor é desativado.

UDP é relatado como `sent_not_acknowledged`; confirme o descarte nos eventos e
contadores do hub. Os testes observam o destino backend e comprovam ausência
de encaminhamento. Para TCP, a CLI distingue rejeição/fechamento de erro de
transporte e sucesso inesperado. Veja o
[README do invasor](../projeto-sockets/sensor_intruder/README.md).

## Diagnóstico

### O dashboard não abre

1. confirme que o daemon Docker está ativo;
2. execute `docker compose ps`;
3. confira se 8501 já está ocupada;
4. leia os logs de `dashboard`, `hub_acesso` e `gateway`.

### Sensores não aparecem

1. verifique gateway e `hub_sensores`;
2. confira os logs do sensor por erros DNS/UDP;
3. consulte motivos como `source_identity_mismatch`, `profile_mismatch` ou
   `device_not_discovered` na auditoria do hub;
4. aguarde o próximo heartbeat. O probe do gateway na backend não recupera
   automaticamente sensores na frontend.

### Comando rejeitado

Verifique se o dispositivo é controlável, possui registro vigente no hub e
aparece com porta 5010 no gateway. Confirme a porta nativa no perfil do hub. O
comando precisa conter IDs e timestamp, apontar para o mesmo alvo nos dois
níveis, solicitar ON/OFF e/ou frequência entre 1 e 60. Relógio muito defasado e
replay do `command_id` também podem causar rejeição.

O gateway também rejeita uma confirmação com `command_id` diferente, estado ou
frequência inválidos, ou valores que não correspondam às alterações solicitadas.

### Consulta analítica vazia ou rejeitada

Confirme nome da métrica, ordem da janela e existência de amostras no período. A
janela não pode exceder `OLAP_MAX_QUERY_WINDOW_SECS`; valores fora da retenção já
podem ter sido removidos.

### Mudança de frequência parece não ter efeito

A alteração é individual por `device_id`. Aguarde ao menos um novo intervalo e
considere o jitter de telemetria. Um sensor em `STATUS_OFF` não publica métricas
operacionais até ser reativado.
