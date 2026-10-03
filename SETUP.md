# Configuração do ambiente de desenvolvimento

O laboratório possui sensores em C, Lua, Java, Python, Go, Rust e TypeScript.
As imagens instalam suas toolchains; para executar a aplicação com Compose,
não é necessário instalar essas linguagens diretamente no host.

## 1. Pré-requisitos e primeira execução

| Opção | Referência | Verificação |
|---|---|---|
| Docker Engine + Compose v2 | Docker 24+ e Compose 2.20+ | `docker compose version` |
| Podman + provider Compose | Suporte a builds, redes internas e healthchecks | `podman compose version` |

No Windows, inicie Docker Desktop ou a máquina do Podman. A partir da raiz do
repositório:

```bash
cd projeto-sockets
docker compose config --quiet
docker compose up --build -d --wait --wait-timeout 180
docker compose ps
```

Abra [o dashboard local](http://127.0.0.1:8501). A configuração padrão anuncia
66 dispositivos: 12 estações C e nove dispositivos por família nas outras seis
linguagens. O [catálogo](docs/metrics.md) descreve as métricas e unidades.

```bash
docker compose logs -f gateway hub_sensores hub_acesso dashboard
docker compose down
```

O volume `gateway_db` preserva o histórico SQLite. `docker compose down --volumes`
remove esse histórico. No encerramento, o gateway interrompe entradas e tenta
drenar os dados admitidos por até `TELEMETRY_SHUTDOWN_TIMEOUT_SECS=20`; o Compose
concede 45 segundos ao serviço. Logs de falha ou prazo excedido indicam que
pacotes ainda em memória podem não ter sido persistidos.

## 2. Topologia e configuração local

Copie `.env.example` para `.env` dentro de `projeto-sockets`:

```bash
cp .env.example .env
```

No PowerShell:

```powershell
Copy-Item .env.example .env
```

O Compose repassa as opções declaradas no `environment` de cada serviço. Uma
variável adicional em `.env` não é injetada automaticamente; inclua-a no Compose
quando o processo precisar recebê-la.

| Rede | Participantes |
|---|---|
| `smart_city_net` | Sensores, dashboard e dois hubs; invasor somente no perfil de teste |
| `gateway_backend` | Gateway e dois hubs; `internal: true` |

O gateway não publica portas e não participa da frontend. `hub_sensores` publica
UDP/5000 e UDP/5002, `hub_acesso` publica TCP/5001, e o dashboard publica HTTP/8501.
O bind padrão é `127.0.0.1`; consulte [SECURITY.md](SECURITY.md) ao alterar a
exposição. As portas de controle 5003, 5004, 5006, 5007, 5008 e 5010 ficam internas.

Nos sensores, `GATEWAY_HOST=hub_sensores`; no dashboard,
`GATEWAY_HOST=hub_acesso`; nos hubs, `GATEWAY_HOST=gateway`. Preserve essa rota.
O hub de acesso aceita apenas a origem DNS/IPv4 `dashboard` por padrão.
Clientes TCP adicionais precisam estar em `ACCESS_ALLOWED_HOSTS`; a publicação
de 5001 no host não concede autorização automática.

Os perfis do hub de sensores relacionam prefixo, tipo, porta nativa e serviço
DNS autorizado. O IP observado precisa corresponder ao serviço. A topologia
usa IPv4 e identidade de rede, sem assinatura ou credencial criptográfica.
Heartbeats recuperam reinicializações; o multicast do gateway fica na backend
e não atravessa automaticamente para sensores na frontend.

## 3. Matriz de ferramentas

| Linguagem/componente | Dependências de desenvolvimento |
|---|---|
| Python 3.11: gateway | `gateway/requirements.txt`: `aiosqlite`, `protobuf` |
| Python 3.11: dashboard | `client/requirements.txt`: `streamlit`, `pandas`, `protobuf` |
| Python 3.11: câmera, hubs, invasor | Manifestos das três pastas; `protobuf` |
| C11/POSIX | Compilador, pthreads, `protobuf-c-compiler`, `libprotobuf-c-dev`, libm |
| Java 21 | JDK para build; JRE e `protobuf-java-3.25.1.jar` para execução |
| Lua 5.4 | LuaRocks, `lua-protobuf`, `luasocket`, `luaposix` |
| Go 1.23 | `sensor_go/go.mod`, `go.sum`, `protoc` e `protoc-gen-go` |
| Rust 1.90+ | Cargo, `sensor_rust/Cargo.lock`, `prost`, compilador Protobuf empacotado |
| TypeScript / Node 22.18+ | Remoção de tipos nativa; sem dependências npm |

Rust gera bindings de `common/messages.proto` no `build.rs`; preserve o lock e
use `--locked`. A imagem final Rust usa usuário sem privilégios. Go verifica
checksums, executa testes com detector de races e produz binário estático para
imagem distroless sem root. Os manifestos Python são instalados com `pip check`.

TypeScript envia descoberta e telemetria com um codec de saída próprio. Os
testes Node validam modelo/runtime; uma suíte Python decodifica fixtures com
bindings gerados de Protobuf, incluindo int64, fixed64 e UTF-8.

## 4. Testes locais por linguagem

Na raiz `projeto-sockets`, use um ambiente virtual Python e `protoc`:

```bash
python -m pip install -r gateway/requirements.txt -r client/requirements.txt -r sensor_python/requirements.txt -r hubs/requirements.txt -r sensor_intruder/requirements.txt
protoc -I=common --python_out=gateway common/messages.proto
protoc -I=common --python_out=client common/messages.proto
protoc -I=common --python_out=sensor_python common/messages.proto
python -m compileall -q gateway client sensor_python hubs sensor_intruder
python -m unittest discover -s tests -v
python -m unittest discover -s sensor_python -p 'test_*.py' -v
```

Os testes usam SQLite temporário e sockets locais, sem exigir Compose. Cobrem
ledger/replay, transações e drenagem, retenção OLAP, correlação e atuação, além
de autorização, descarte, relay e encerramento dos hubs e cenários do invasor.
`compileall` valida sintaxe sem importar os módulos.

Com GCC disponível, na mesma raiz:

```bash
gcc -std=c11 -Wall -Wextra -Werror sensor_c/test_aqi.c -lm -o test-aqi
./test-aqi
gcc -std=c11 -Wall -Wextra -Werror sensor_c/test_environment.c -lm -o test-environment
./test-environment
```

No Windows, use nomes terminados em `.exe` e execute com `./` ou `.\`. A
compilação completa do sensor exige também POSIX e Protobuf-C.

Na pasta `sensor_go`, após gerar o binding com `protoc-gen-go` disponível:

```bash
mkdir -p proto
protoc --proto_path=../common --go_out=./proto --go_opt=paths=source_relative ../common/messages.proto
go mod verify
go vet ./...
go test ./...
```

Na pasta `sensor_rust`, com Rust 1.90 ou superior:

```bash
cargo test --locked
cargo build --release --locked
```

Na raiz `projeto-sockets`, com Node 22.18+, Python e `protoc` ou `grpcio-tools`:

```bash
node --experimental-strip-types --test sensor_typescript/tests/model.test.ts sensor_typescript/tests/runtime.test.ts
python -m unittest discover -s sensor_typescript/tests -p 'test_*.py' -v
javac sensor_java/TrafficSample.java sensor_java/TrafficSampleTest.java
java -cp sensor_java TrafficSampleTest
```

São oito testes Node e duas verificações Python do codec. O teste Java de
amostras não exige Protobuf; seu Dockerfile compila o sensor completo depois.
Na pasta `sensor_lua`, execute `lua5.4 test_lamp_metrics.lua`. As verificações
C, Lua, Java, Go, Rust e TypeScript também fazem parte de seus builds.

## 5. Validação integrada e invasor

Na raiz `projeto-sockets`:

```bash
docker compose config --quiet
docker compose build
docker compose up -d --wait --wait-timeout 180
curl --fail http://127.0.0.1:8501/_stcore/health
docker compose exec dashboard python smoke.py --expected-devices 66
docker compose --profile security-test run --build --rm sensor_invasor
docker compose logs --since=2m hub_sensores hub_acesso
docker compose down
```

O smoke test executa dentro do dashboard autorizado e usa os hubs para consultar
a frota e testar atuação. Ajuste a expectativa de dispositivos se mudou as
contagens.

O invasor fica desligado fora do perfil `security-test`; seu processo também
exige `INTRUDER_ENABLED=1`. Emite 12 cenários finitos, nove UDP e três TCP,
somente aos dois destinos privados/loopback configurados. Não há varredura nem
comando válido capaz de modificar os sensores. O envio UDP é registrado como
`sent_not_acknowledged`: confirme descarte nos logs/contadores dos hubs e nos
testes de integração, não apenas na saída do emissor.

O workflow [ci.yml](.github/workflows/ci.yml) reúne as suítes, builds, smoke test
e emissão invasora. Uma verificação declarada no CI ainda precisa executar com
sucesso; registre quais toolchains, testes e imagens foram realmente validados
e quais permaneceram indisponíveis no ambiente local.

## 6. Podman no Windows

Se `podman compose` não encontrar provider, configure `podman-compose` e
`PODMAN_COMPOSE_PROVIDER=podman-compose`. A partir da raiz do repositório:

```powershell
.\projeto-sockets\scripts\podman-compose.ps1 up --build
```

O Dockerfile de fallback em `projeto-sockets/Dockerfile` deve acompanhar os
Dockerfiles específicos, inclusive os novos hubs, invasor, Rust e TypeScript.
Verifique se o provider aplica `internal: true`, os perfis e healthchecks.

## 7. Diagnóstico

- **Arquivo Compose não encontrado:** entre em `projeto-sockets` ou informe
  `-f projeto-sockets/docker-compose.yml` a partir da raiz.
- **Daemon indisponível:** inicie Docker Desktop ou a máquina do Podman e
  verifique `docker version` ou `podman info`.
- **Primeiro build demorado:** compiladores e dependências são baixados nessa
  execução; builds posteriores aproveitam cache.
- **Sensor não aparece:** verifique `hub_sensores`, DNS do serviço e eventos
  `source_identity_mismatch`, `profile_mismatch` e `device_not_discovered`.
  Aguarde o heartbeat; não dependa do probe backend para alcançar a frontend.
- **Cliente TCP rejeitado:** confira `ACCESS_ALLOWED_HOSTS` e o IPv4 realmente
  visto pelo hub na auditoria. A interface web usa o dashboard já autorizado.
- **Falha de drenagem:** investigue SQLite, espaço, retries e o prazo de
  encerramento. Aumentar esse prazo exige margem correspondente no Compose.

Mais detalhes estão em [Operações](docs/operations.md).
