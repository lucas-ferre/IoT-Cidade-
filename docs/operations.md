# Operações

[Documentação](README.md) · [Arquitetura](architecture.md) · [Protocolo](protocol.md)

## Pré-requisitos

Para a execução padrão, use Docker Desktop com Compose v2 ou Podman com suporte
a Compose. As toolchains de Python, C, Lua, Java, Go e Protobuf são instaladas
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
docker compose logs -f gateway dashboard sensor_estacionamento
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
| `gateway` | `127.0.0.1:5000` | UDP | Telemetria |
| `gateway` | `127.0.0.1:5001` | TCP | Cliente/dashboard |
| `gateway` | `127.0.0.1:5002` | UDP | Descoberta |
| `dashboard` | `127.0.0.1:8501` | HTTP/TCP | Interface Streamlit |

### Somente na rede Compose

| Serviço | Porta | Transporte | Uso |
|---|---:|---|---|
| `sensor_semaforo` | 5003 | TCP | Controle Java |
| `sensor_camera` | 5004 | TCP | Controle Python |
| Multicast dos sensores | 5005 | UDP | Recuperação de descoberta |
| `sensor_posto` | 5006 | TCP | Controle Lua |
| `sensor_estacionamento` | 5007 | TCP | Controle Go |

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
| `C_DEVICE_COUNT` | `6` | Estações ambientais no processo C |
| `LUA_DEVICE_COUNT` | `3` | Postes no processo Lua |
| `JAVA_DEVICE_COUNT` | `3` | Semáforos no processo Java |
| `CAMERA_DEVICE_COUNT` | `3` | Câmeras no processo Python |
| `GO_PARKING_DEVICE_COUNT` | `3` | Estacionamentos no processo Go, entre 1 e 100 |

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

O sensor Go também aceita `GATEWAY_HOST` (`gateway`), `SENSOR_HOSTNAME`
(`sensor_estacionamento`) e `SENSOR_HEALTHCHECK_ADDRESS` para diagnósticos
específicos. Em uma execução normal pelo Compose, os padrões já são adequados.

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

O gateway testa 5001/TCP; o dashboard testa sua porta web; sensores controláveis
testam os listeners TCP; o sensor C verifica o processo. Um container `healthy`
confirma disponibilidade local do processo, não a correção semântica de todas as
métricas.

### Logs

```bash
docker compose logs --since=10m gateway
docker compose logs -f sensor_estacionamento
docker compose logs --tail=200 dashboard
```

Investigue especialmente:

- mensagens rejeitadas por validação;
- descartes da fila de telemetria ou do limite de descoberta;
- timeouts ao encaminhar comandos;
- dispositivos marcados offline;
- falhas de checkpoint ou retenção SQLite.
- retries de escrita e prazo excedido ao drenar telemetria no encerramento.

### Banco de dados

A imagem do gateway contém Python, portanto é possível consultar SQLite sem o
executável `sqlite3`:

```bash
docker compose exec gateway python -c "import sqlite3; db=sqlite3.connect('/app/db/smartcity_gateway.db'); print(db.execute('SELECT device_id, type, status, last_seen FROM devices ORDER BY device_id').fetchall())"
```

Para uma inspeção mais longa, prefira copiar o banco e usar uma ferramenta local.
Faça isso com o gateway parado ou copie também os arquivos WAL/SHM para obter um
snapshot consistente.

## Validação e testes

### Verificações rápidas

Na raiz `projeto-sockets`, use um ambiente Python com os três manifestos e
`protoc` disponível. Gere os bindings antes de executar a suíte:

```bash
docker compose config --quiet
python -m pip install -r gateway/requirements.txt -r client/requirements.txt -r sensor_python/requirements.txt
protoc -I=common --python_out=gateway common/messages.proto
protoc -I=common --python_out=client common/messages.proto
protoc -I=common --python_out=sensor_python common/messages.proto
python -m compileall -q gateway/main.py gateway/analytics.py client/app.py sensor_python/sensor.py
python -m unittest discover -s tests -v
python -m unittest discover -s sensor_python -p 'test_*.py' -v
```

Os testes Python cobrem OLAP e retenção, persistência e replay, falhas/cancelamento,
drenagem, framing e correlação TCP, gráficos e classificação do dashboard, além
de validação e atuação do sensor Python. Usam SQLite temporário e sockets locais;
não exigem os serviços Compose em execução.

### Cálculo AQI em C

Com GCC disponível, execute na raiz `projeto-sockets`:

```bash
gcc -std=c11 -Wall -Wextra -Werror sensor_c/test_aqi.c -lm -o test-aqi
./test-aqi
```

O teste cobre limites, lacunas entre faixas e monotonicidade do AQI. No Windows,
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

### Smoke test integrado

```bash
docker compose up --build --wait
docker compose ps
curl --fail http://localhost:8501/_stcore/health
docker compose logs --since=2m gateway sensor_estacionamento
```

Confirme no dashboard que os dispositivos `parking_*` aparecem como tipo
Estacionamento Inteligente, publicam cinco métricas e respondem a ON/OFF e à
mudança de frequência.

O CI instala as dependências Python, gera os três bindings, executa ambas as
suítes Python e o teste C, depois constrói as imagens e realiza o smoke test.
Um teste unitário aprovado não confirma que o ambiente completo foi implantado.

## Diagnóstico

### O dashboard não abre

1. confirme que o daemon Docker está ativo;
2. execute `docker compose ps`;
3. confira se 8501 já está ocupada;
4. leia os logs de `dashboard` e `gateway`.

### Sensores não aparecem

1. verifique se o gateway está saudável;
2. confira os logs do sensor por erros DNS/UDP;
3. aguarde o próximo heartbeat ou probe multicast;
4. verifique se o runtime/host permite multicast na rede bridge.

### Comando rejeitado

Verifique se o dispositivo é controlável e está registrado com porta válida. O
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
