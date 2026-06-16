# Smart City — Plataforma Distribuída de Monitoramento Urbano

A **Smart City** é uma plataforma de telemetria, autenticação e controle em tempo real para dispositivos IoT distribuídos em ambiente urbano. Com arquitetura orientada a eventos e agentes híbridos, o ecossistema acomoda sensores construídos em diversas linguagens (C, Lua, Java, Python) e realiza a ingestão massiva de dados através de agregadores distribuídos de borda e um Gateway central.

---

## 📖 Sumário

1. [Visão Geral da Arquitetura](#-visão-geral-da-arquitetura)
2. [Fluxo de Ciclo de Vida e Rede](#-fluxo-de-ciclo-de-vida-e-rede)
3. [Stack Tecnológico](#-stack-tecnológico)
4. [Componentes do Sistema](#-componentes-do-sistema)
5. [Segurança e Autenticação](#-segurança-e-autenticação)
6. [Catálogo de Variáveis de Ambiente](#-catálogo-de-variáveis-de-ambiente)
7. [Tratamento de Exceções e Resiliência](#-tratamento-de-exceções-e-resiliência)
8. [Catálogo de Sensores e Métricas](#-catálogo-de-sensores-e-métricas)
9. [Iniciando o Ambiente](#-iniciando-o-ambiente)

---

## 🏗️ Visão Geral da Arquitetura

O sistema transcende o modelo clássico de *Hub-and-Spoke*. Ele adota um padrão de **Agregadores Distribuídos de Borda** amparados por um **Message Broker Central (Redis)**, oferecendo roteamento dinâmico, alta escalabilidade e isolamento de carga.

```text
                          Rede Compose: smart_city_net
┌────────────────────────────────────────────────────────────────────┐
│                                                                    │
│  ┌──────────────────┐   TCP :5007 (Autenticação)                  │
│  │  sensor_clima    │──────────────────────────────┐              │
│  │  C · 6 Estações  │   UDP Dinâmica (Telemetria)  │              │
│  │  Pici/Benf/Poran.│───────▶ Agregador Rust       │              │
│  └──────────────────┘                              ▼              │
│                                          ┌──────────────────────┐ │
│  ┌──────────────────┐   TCP :5007       │      gateway         │ │
│  │  sensor_posto    │─────────────────▶ │  Python / asyncio    │ │
│  │  Lua · 3 Postes  │◀── TCP :5006 ──────│                      │ │
│  └──────────────────┘                    │  SQLite WAL          │ │
│                                          │  Pool aiosqlite      │ │
│  ┌──────────────────┐                    │  Servidor TCP :5007  │ │
│  │ Agregador Java   │◀── Redis PubSub ──│  Worker Redis        │ │
│  │ Netty            │──▶ Redis Streams ─│                      │ │
│  └──────────────────┘                    │  :5001/TCP cliente   │ │
│                                          └──────────────────────┘ │
│  ┌──────────────────┐                              ▲              │
│  │ Agregador Rust   │◀── Redis PubSub ─────────────┘              │
│  │ Tokio            │──▶ Redis Streams                            │
│  └──────────────────┘                                             │
│                                                                    │
│  ←── Multicast 239.0.0.1 ────────────────────────────────────────  │
│       Aggregator Load (UDP :5005)                                 │
└────────────────────────────────────────────────────────────────────┘
                                                     │ :8501
                                                     ▼
                                               Navegador Web
```

---

## 🔄 Fluxo de Ciclo de Vida e Rede

O ciclo de vida de cada sensor abrange etapas robustas de roteamento e segurança, lidando graciosamente com falhas e reconexões.

| Fase | Protocolo | Descrição da Ação |
|------|-----------|------------------|
| **1. Descoberta de Borda** | UDP :5005 | Sensores "escutam" em Multicast `AggregatorLoad` para mensurar métricas de rede e escolher o melhor Agregador (menor fila, menor CPU) no ecossistema. |
| **2. Registro (Discovery)** | UDP :5002 | O sensor envia seu `DiscoveryResponse` de presença para a porta de descoberta fixa do agregador eleito. O agregador criptografa e envia para o Redis (`discovery_stream`). |
| **3. Autenticação** | TCP :5007 | O sensor contata o Gateway fornecendo a **Chave de Licença** + **Código Hexadecimal** de serviço. Em caso de sucesso, o Gateway devolve uma porta UDP dinamicamente alocada. |
| **4. Orquestração Interna**| Redis PubSub| Simultaneamente, o Gateway envia um comando `ABRIR_PORTA_UDP: <porta>` para o Agregador via canal do Redis (ex: `agg_control_rust_1`). |
| **5. Telemetria** | UDP Dinâmico | O sensor injeta `DataPayload` (Protobuf) de métricas ativamente na sua nova porta UDP privada do Agregador. O agregador repassa para o Redis (`telemetry_stream`). |
| **6. Persistência (OLAP)**| I/O File | O Gateway lê as Streams do Redis de forma assíncrona, faz lotes (*batching*) e persiste na base de dados analítica (SQLite WAL Mode + Rollups automáticos). |
| **7. Limpeza (Unbind)** | Redis PubSub| Quando um sensor cai ou fica ocioso por determinado _timeout_, o Gateway detecta via Garbage Collector, recicla o slot e despacha `FECHAR_PORTA_UDP: <porta>`, garantindo alívio do Socket OS (Descritores de Arquivos). |

---

## 🛠️ Stack Tecnológico

| Componente | Lote Tecnológico | Ponto Forte / Função |
|------------|------------------|----------------------|
| **Gateway Central** | `Python 3.11` + `asyncio` | Roteamento, Motor OLAP, Batching assíncrono com `aiosqlite`. |
| **Agregador Rust** | `Rust` + `Tokio` | Altíssima performance no tratamento de pacotes UDP. Baixo consumo de RAM. |
| **Agregador Java** | `Java 21` + `Netty` | Robustez corporativa, EventLoops reativos NIO para streams de datagramas. |
| **Message Broker** | `Redis 7` | Encapsula streams analíticas e gerencia pub/sub de controle em nanossegundos. |
| **Persistência** | `SQLite 3` (WAL) | Transações isoladas, rollups otimizados para séries temporais e estatística. |
| **Dashboard** | `Streamlit` | Frontend em Python para gráficos, consultas analíticas (OLAP) e controle visual. |
| **Criptografia** | `AES-128 GCM` | Confidencialidade e integridade no tráfego interno dos agregadores. |

---

## 🛡️ Segurança e Autenticação

### 1. Hardening e Handshake
Na fase de inicialização, cada sensor deve transpor o servidor TCP em `:5007` do Gateway. A credencial é composta por:
- **`license_key_part`**: Parte válida de uma chave de licenciamento (e.g., `SMARTCITY-V1-FULL-LICENSE`).
- **`hex_service_code`**: Assinatura criptografada e atrelada ao tipo de aparelho físico que está sendo rodado:
  - Estação Ambiental (`0C`)
  - Semáforo (`0A`)
  - Poste Inteligente (`0B`)
  - Câmera Analítica (`0D`)

### 2. Criptografia no Pipeline de Dados
Toda a telemetria que transita do Agregador para o Gateway é fortificada com encriptação simétrica **AES-128-GCM**.
- Os agregadores produzem **nonces aleatórios (12 bytes)** e despacham as mensagens pelo barramento Redis. O Gateway as decifra dinamicamente no destino. O segredo principal fica unicamente na variável `AES_SECRET_KEY`.

---

## 🗄️ Catálogo de Variáveis de Ambiente

Todas as parametrizações ocorrem no arquivo `docker-compose.yml` sem necessidade de mexer no código ou recompilar os microsserviços.

### Comuns / Infraestrutura
| Variável | Padrão | Explicação |
|----------|--------|------------|
| `AES_SECRET_KEY` | `SmartCityKey1234` | String para seed da chave de encriptação dos pacotes internos (GCM). |
| `REDIS_HOST` e `PORT`| `redis` / `6379` | Apontamento da rede para o Message Broker. |

### Gateway Coordenador
| Variável | Padrão | Explicação |
|----------|--------|------------|
| `GATEWAY_LICENSE_KEY`| `SMARTCITY-V1-FULL-LICENSE`| A string master exigida durante a autenticação de dispositivos TCP. |
| `MAX_AVAILABLE_PORTS`| `100` | Quantidade máxima de portas UDP alocáveis na rede por agregador simultaneamente. |
| `DEVICE_OFFLINE_TIMEOUT_SECS`| `45` | Segundos tolerados sem `heartbeat` ou métricas antes de ativar a reciclagem da porta e marcar status `OFFLINE`. |
| `DB_POOL_SIZE` | `4` | Conexões de cursor assíncronas do pool `aiosqlite`. |
| `TELEMETRY_QUEUE_MAXSIZE`| `10000` | Tamanho máximo da RAM consumida antes de aplicar _Backpressure_ ao Redis. |
| `METRICS_RAW_RETENTION_SECS`| `604800` (7d) | TTL (Time-to-Live) em segundos da base crua de estatísticas (evitar superlotar disco). |
| `ROLLUP_1H_RETENTION_SECS`| `0` | Se 0, garante retenção infinita do agrupamento por hora (para dashboard e longo prazo). |

### Sensores (Agentes IoT)
| Variável | Padrão | Explicação |
|----------|--------|------------|
| `SENSOR_LICENSE_PART`| `V1-FULL` | Assinatura pass-through exigida pelos servidores IoT (`Sensor Clima`, `Sensor Semáforo`, etc). |
| `X_DEVICE_COUNT` | `3` a `6` | Escala vertical dos sensores (uma imagem container simula até _N_ postes, câmeras, etc). |
| `SENSOR_HEARTBEAT_INTERVAL_SECS`| `10` | Frequência que o hardware "pulsa" no agregador para provar que está online. |

---

## 🚨 Tratamento de Exceções e Resiliência

### Recuperações Clássicas Implementadas
O sistema contém mecanismos de blindagem para manter a disponibilidade:

- `InvalidLicenseKeyException` | `InvalidHexServiceCodeException`: Rejeita clientes maliciosos no servidor Socket no handshake de autenticação. Fecha conexão imediatamente poupando CPU.
- `NoPortsAvailableException`: Protege o OS. Impede a alocação de infinitas conexões UDP dinâmicas em caso de sobrecarga imprevista, paralisando alocações na faixa delimitada (`MAX_AVAILABLE_PORTS`).
- **Port Exhaustion Prevention** (Unbind Automático): Se o sensor falha silenciosamente, o GC varre e envia mensagem efêmera `FECHAR_PORTA_UDP`. Agregadores então libertam o `File Descriptor` e IP stack bind.
- **Fallbacks OLAP**: O motor estatístico do SQLite consulta tabelas sumarizadas (Rollup 5m, 1h). Contudo, se a agregação ainda não obteve dados massivos o suficiente, ele executa um fallback resiliente em tempo real (on-the-fly) retornando as planilhas brutas.

---

## 📊 Catálogo de Sensores e Métricas

Todas as simulações incorporam atrasos probabilísticos (Jitter), evitando sincronização de relógios (efeito de rajada).

| Sensor (Tecnologia) | Métricas Simuladas | Disparos Excepcionais (Limiar de Alarme) |
|---------------------|-------------------|---------------------------------------|
| **Câmera de Tráfego** (Python) | `vehicles_count` (12–95), `infractions` (0–5) | Fluxo > 80 veíc/min, ou > 3 infrações seguidas. |
| **Estação Ambiental** (C) | `temperature`, `humidity`, `co2`, `pm25`, `pm10`, `aqi` | Temp > 32°C ou PM2.5 alarmante (Insalubridade). |
| **Semáforo** (Java) | `state` (Ciclo), `queue_length` (Tamanho da Fila) | Fila acima de 35 carros. |
| **Poste Inteligente** (Lua) | `luminosity` (%), `power_consumption` (W) | Consumo acima de 32W ou queima de lâmpada (<80%). |

> A Telemetria e Descobertas trafegam via **UDP**, enquanto Requisições de Controle partindo do usuário via Dashboard (Ligar/Desligar remoto) trafegam via **TCP**.

---

## 🚀 Iniciando o Ambiente

A plataforma inteira subirá com um simples orquestrador, resolvendo as dependências internas por conta própria através de scripts healthcheck automáticos.

**Passo Único (Via Docker ou Podman)**:
Na pasta raiz do projeto (`projeto-sockets`), onde fica seu `docker-compose.yml`:

```bash
# Rodar e deixar os logs engatados no terminal em tempo real
docker compose up --build

# Ou, se desejar subir os serviços em background
docker compose up --build -d
```

### Serviços Acessíveis
1. **Dashboard UI** — Abra [http://localhost:8501](http://localhost:8501) no seu navegador predileto para visualizar tudo, comandar robôs e checar análises.
2. **Inspeção de Bancos de Dados** (Caso possua sqlite3 na máquina):
   ```bash
   docker exec gateway sqlite3 db/smartcity_gateway.db "SELECT * FROM devices;"
   ```

Aproveite o ambiente Smart City em sua estabilidade máxima!