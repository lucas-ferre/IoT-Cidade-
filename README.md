# Smart City — Distributed Urban Monitoring

[![CI](https://github.com/lucas-ferre/projeto_socket/actions/workflows/ci.yml/badge.svg)](https://github.com/lucas-ferre/projeto_socket/actions/workflows/ci.yml)

Laboratório distribuído de **telemetria, descoberta, controle remoto e análise de
dados urbanos**. Sensores em **C, Lua, Java, Python, Go, Rust e TypeScript**
compartilham um contrato Protocol Buffers. Dois hubs filtram a comunicação com o
gateway assíncrono, que persiste os dados no SQLite e atende ao dashboard Streamlit.

A configuração padrão possui **66 dispositivos**, **62 nomes distintos de
métricas** e até **624 valores por rodada completa** de leituras. Dispositivos
desligados continuam anunciando presença, mas não produzem leituras nessa condição.

## Arquitetura

O desenho abaixo usa texto simples e largura reduzida para leitura no GitHub.
As setas representam o caminho lógico entre os componentes; as respostas e os
comandos percorrem essas mesmas conexões.

```text
Sensores (7 linguagens)
         |
         v
    hub_sensores
         |
         v
Gateway <---- hub_acesso <---- Dashboard
   |
   v
SQLite
```

| Fluxo | Caminho | Transporte |
|---|---|---|
| Descoberta e heartbeat | sensor → hub_sensores → gateway | UDP 5002 |
| Telemetria | sensor → hub_sensores → gateway → SQLite | UDP 5000 |
| Inventário e análises | dashboard → hub_acesso → gateway | TCP 5001 |
| Controle e confirmação | dashboard → hub_acesso → gateway → hub_sensores → sensor | TCP 5001, proxy 5010 e porta nativa |

**`hub_sensores`** verifica o IP de origem contra os serviços autorizados, o
prefixo do dispositivo, seu tipo, a porta de controle e o conteúdo dos datagramas.
Ele mantém as rotas de controle e anuncia ao gateway a porta do proxy `5010`.
O endereço declarado pelo sensor não é usado como destino confiável de controle.

**`hub_acesso`** admite as origens configuradas em `ACCESS_ALLOWED_HOSTS`
(`dashboard` por padrão), valida os pedidos Protobuf e encaminha consultas e
comandos. Ambos aplicam limites de tamanho, tempo, taxa e concorrência e produzem
auditoria JSON com o motivo das rejeições.

| Rede do Compose | Participantes |
|---|---|
| `smart_city_net` | sensores, dashboard, hubs e simulador invasor opcional |
| `gateway_backend` — interna | gateway e os dois hubs |

O gateway não publica portas no host. O dashboard (`8501`), as entradas UDP do
hub de sensores (`5000` e `5002`) e o hub de acesso (`5001`) são publicados em
`127.0.0.1` por padrão. A porta do hub de acesso continua sujeita à autorização
de origem; publicar a porta não autoriza um cliente externo automaticamente.

## Frota e métricas

| Serviço | Linguagem | Dispositivos | Métricas por leitura | Domínio |
|---|---|---:|---:|---|
| `sensor_clima` | C | 12 | 13 | clima, partículas, qualidade do ar e ruído |
| `sensor_posto` | Lua | 9 | 8 | iluminação, consumo e condições elétricas |
| `sensor_java` | Java | 9 | 8 | semáforos, filas e fluxo de pedestres |
| `sensor_camera` | Python | 9 | 8 | tráfego, infrações e confiança da detecção |
| `sensor_estacionamento` | Go | 9 | 11 | vagas, rotatividade, receita e recarga elétrica |
| `sensor_agua` | Rust | 9 | 8 | reservatórios, vazão, pressão e qualidade da água |
| `sensor_lixeiras` | TypeScript | 9 | 9 | resíduos, bateria, sinal e condições da lixeira |
| **Total** | **7 linguagens** | **66** | **624 valores por rodada** | |

O [catálogo de métricas](docs/metrics.md) descreve cada nome e unidade.
As simulações preservam relações entre grandezas: vagas ocupadas mais disponíveis
igualam o total, veículos em recarga cabem nas vagas ocupadas, e tensão, corrente e
potência dos postes são calculadas em conjunto.

O novo [sensor Rust](projeto-sockets/sensor_rust/README.md) usa bindings gerados do
contrato central, descoberta e heartbeat UDP, controle individual TCP, proteção
contra replay e encerramento gracioso. Os dispositivos `water_*` podem ser
ligados, desligados e ter seu intervalo ajustado entre 1 e 60 segundos.

O novo [sensor TypeScript](projeto-sockets/sensor_typescript/README.md) executa em
Node.js sem dependências npm. Publica os dispositivos `waste_*`, usa um codec
Protobuf validado contra os bindings Python e drena os envios durante o
encerramento. Clima e lixeiras publicam continuamente e não expõem controle TCP.

## Persistência e consultas

O gateway combina `asyncio`, fila limitada, lotes de escrita e SQLite em WAL. O
ledger `(device_id, message_id)` e o checkpoint de ordem por dispositivo são
confirmados na mesma transação das métricas e dos rollups. Retransmissões e
reinicializações não duplicam leituras já confirmadas; lotes com falha de escrita
são repetidos. Os IDs expiram somente quando `MESSAGE_MAX_AGE_SECS > 0`.

As consultas calculam média, desvio-padrão amostral e variação máxima. A fonte
analítica considera duração e retenção do período; os metadados identificam a
resolução e a janela efetiva. Os limites padrão são 30 dias e 2.000 pontos por
consulta. Gráficos usam datas completas em UTC para preservar leituras de dias
diferentes.

Requisições e confirmações possuem identificadores correlacionados. O gateway
confere o ID do comando e os valores de estado e frequência devolvidos pelo
sensor antes de reportar sucesso.

## Início rápido

Pré-requisitos: Docker Engine com Compose v2 ou uma configuração compatível do
Podman. Os runtimes dos sensores são preparados pelas imagens.

```bash
git clone https://github.com/lucas-ferre/projeto_socket.git
cd projeto_socket/projeto-sockets
docker compose config --quiet
docker compose up --build --detach --wait
```

Abra [o dashboard local](http://127.0.0.1:8501). Para verificar o caminho completo
das sete famílias até o banco, incluindo um comando ao sensor Rust:

```bash
docker compose exec -T dashboard python smoke.py --expected-devices 66
```

Esse teste aguarda descoberta e leituras, consulta séries pelo hub de acesso e
reaplica o intervalo padrão de 5 segundos a um dispositivo Rust. Em uma frota
personalizada, ajuste `--expected-devices` para o total configurado.

```bash
docker compose ps
docker compose logs --follow gateway hub_sensores hub_acesso
docker compose down
```

O volume `gateway_db` preserva o histórico. `docker compose down --volumes` também
remove os dados simulados. Em `SIGTERM`/`SIGINT`, o gateway interrompe novas entradas
e tenta drenar a fila antes de fechar o banco: prazo padrão de 20 segundos, com
45 segundos de tolerância no Compose. Prazo excedido ou falha final de escrita
geram erro no log; pacotes ainda em memória podem não ser persistidos.

### Configuração

```bash
cp .env.example .env
```

No PowerShell, use `Copy-Item .env.example .env`. O arquivo permite ajustar as
quantidades de sensores, os limites dos hubs, a ingestão e a retenção. A referência
está em [Operações](docs/operations.md).

## Sensor invasor para testes

O serviço `sensor_invasor` pertence ao perfil opcional `security-test` e só executa
quando solicitado. Uma rodada contém **12 cenários limitados: 9 envios UDP e
3 pedidos TCP**, incluindo identidade desconhecida, tentativa de imitar um
dispositivo existente, mensagem malformada, timestamp inválido e métrica não finita.

Com o laboratório em execução:

```bash
docker compose --profile security-test run --build --rm sensor_invasor
docker compose logs --since 2m hub_sensores hub_acesso
docker compose exec -T dashboard python smoke.py --expected-devices 66
```

O simulador aceita somente destinos privados ou loopback e não envia comandos
válidos de atuação. Sua saída distingue rejeições TCP de datagramas UDP enviados
sem confirmação. A prova de bloqueio UDP vem da auditoria dos hubs e dos testes de
integração, que verificam que inventário, rotas e tabelas do SQLite permanecem
intactos após os 12 cenários. Veja o [simulador](projeto-sockets/sensor_intruder/README.md).

## Dashboard

1. **Fontes de dados:** inventário e presença dos dispositivos.
2. **Painel de atuação:** estado e frequência por dispositivo controlável.
3. **Consultas analíticas:** seleção das 62 métricas e operações por intervalo.
4. **Inspeção individual:** métricas de cada família, série temporal e eventos.

O [catálogo compartilhado](projeto-sockets/client/metric_catalog.py) mantém
rótulos, unidades e opções de consulta consistentes, inclusive para água e lixeiras.

## Validação

Os testes de integração exercitam UDP e TCP reais em loopback, os dois hubs, o
gateway e um banco SQLite temporário. Incluem descoberta, consulta persistente,
telemetria, controle com ACK e os cenários do invasor.

```bash
cd projeto-sockets
python -m pip install -r gateway/requirements.txt -r client/requirements.txt -r sensor_python/requirements.txt -r hubs/requirements.txt -r sensor_intruder/requirements.txt
protoc -I=common --python_out=gateway common/messages.proto
protoc -I=common --python_out=client common/messages.proto
protoc -I=common --python_out=sensor_python common/messages.proto
python -m unittest discover -s tests -v
python -m unittest discover -s sensor_python -p 'test_*.py' -v
node --experimental-strip-types --test sensor_typescript/tests/*.test.ts
python -m unittest discover -s sensor_typescript/tests -p 'test_*.py' -v
```

Para Rust:

```bash
cd sensor_rust
cargo fmt --check
cargo test --locked
cargo build --locked
```

Os comandos C, Lua, Java e Go estão em [Operações](docs/operations.md#validação-e-testes)
e [Setup](SETUP.md). Os testes das simulações verificam faixas, unidades e relações
entre métricas. O codec TypeScript é decodificado pelo Protobuf Python nos testes
de compatibilidade.

O [workflow de CI](.github/workflows/ci.yml) prepara as linguagens, executa as
regressões, constrói os 11 serviços padrão e inicia o Compose. Depois verifica a
saúde do dashboard, consulta dados das sete famílias, testa controle Rust através
dos hubs e executa o simulador invasor. A imagem Go também executa testes com
detector de condições de corrida.

## Documentação

| Documento | Conteúdo |
|---|---|
| [Índice técnico](docs/README.md) | mapa do código e fontes de verdade |
| [Arquitetura](docs/architecture.md) | redes, hubs, fluxos e persistência |
| [Protocolo](docs/protocol.md) | mensagens, framing, portas e validações |
| [Métricas](docs/metrics.md) | catálogo das sete famílias e unidades |
| [Operações](docs/operations.md) | configuração, auditoria, testes e diagnóstico |
| [Setup](SETUP.md) | preparação detalhada do ambiente |
| [Segurança](SECURITY.md) | modelo de ameaça e limites conhecidos |
| [Contribuição](CONTRIBUTING.md) | critérios para mudanças verificáveis |

## Estrutura

```text
.
├── .github/workflows/ci.yml
├── docs/
├── CONTRIBUTING.md
├── SECURITY.md
├── SETUP.md
└── projeto-sockets/
    ├── common/messages.proto
    ├── gateway/
    ├── hubs/
    ├── client/
    ├── sensor_c/
    ├── sensor_lua/
    ├── sensor_java/
    ├── sensor_python/
    ├── sensor_go/
    ├── sensor_rust/
    ├── sensor_typescript/
    ├── sensor_intruder/
    ├── tests/
    └── docker-compose.yml
```

## Escopo de segurança

Este é um laboratório acadêmico para execução local. Os hubs restringem origens
por DNS/IP e perfil de dispositivo, validam mensagens e limitam recursos. Essas
verificações não fornecem identidade criptográfica: UDP/TCP não possuem TLS,
mTLS nem assinatura das mensagens. O isolamento do gateway no Compose e os testes
do invasor cobrem o cenário descrito em [SECURITY.md](SECURITY.md).

## Decisões mantidas em aberto

- **Ponto 1 — estratégia definitiva de build:** manter Dockerfiles por serviço,
  consolidar o fallback ou adotar outra organização.
- **Ponto 9 — licença:** escolher a licença compatível com o objetivo do portfólio.

Até a escolha do ponto 9, a ausência de um arquivo `LICENSE` significa que não há
uma permissão aberta de reutilização concedida pelo repositório.
