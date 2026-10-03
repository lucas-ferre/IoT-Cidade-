# Configuração do ambiente de desenvolvimento

Este documento descreve a preparação, execução e validação local do sistema
distribuído. O caminho recomendado utiliza contêineres: não é necessário instalar
Python, C, Java, Lua, Go ou Protobuf diretamente no host para executar a aplicação.

## 1. Pré-requisitos

Use um dos runtimes abaixo:

| Opção | Versão de referência | Verificação |
|---|---:|---|
| Docker Engine + Compose v2 | Docker `>= 24`, Compose `>= 2.20` | `docker compose version` |
| Podman + provider Compose | instalação atual com suporte a Compose | `podman compose version` |

No Windows, confirme que Docker Desktop ou a máquina do Podman está em execução
antes de iniciar o ambiente.

## 2. Primeira execução

Os comandos abaixo partem da raiz do repositório:

```bash
cd projeto-sockets
docker compose config --quiet
docker compose up --build -d --wait
docker compose ps
```

Quando todos os healthchecks estiverem saudáveis, abra
<http://127.0.0.1:8501>. Para acompanhar ou encerrar o ambiente:

```bash
docker compose logs -f
docker compose down
```

O volume `gateway_db` preserva o SQLite entre reinicializações. Para apagar também
os dados da simulação, use conscientemente `docker compose down --volumes`.

Ao receber `SIGTERM` ou `SIGINT`, o gateway fecha as entradas e drena a telemetria
admitida antes de encerrar o banco. A espera usa
`TELEMETRY_SHUTDOWN_TIMEOUT_SECS=20`, e o Compose concede 45 segundos ao serviço.
Se o prazo terminar ou a escrita não recuperar, consulte os erros de drenagem nos
logs: pacotes ainda em memória podem não ter sido gravados.

## 3. Configuração local

O Compose possui valores seguros por padrão. Para personalizar a simulação, copie o
arquivo de exemplo sem versionar a cópia:

```bash
cp .env.example .env
```

No PowerShell:

```powershell
Copy-Item .env.example .env
```

As opções iniciais controlam o nível de log, o número de dispositivos virtuais e o
endereço de publicação. Mantenha `BIND_ADDRESS=127.0.0.1`; consulte
[SECURITY.md](SECURITY.md) antes de expor portas em outra interface.

## 4. Matriz de linguagens e dependências

### Python 3.11

| Serviço | Manifesto | Dependências diretas |
|---|---|---|
| Gateway | `projeto-sockets/gateway/requirements.txt` | `aiosqlite`, `protobuf` |
| Dashboard | `projeto-sockets/client/requirements.txt` | `streamlit`, `pandas`, `protobuf` |
| Sensor de câmera | `projeto-sockets/sensor_python/requirements.txt` | `protobuf` |

Os Dockerfiles instalam esses manifestos e executam `python -m pip check`. Para
desenvolvimento fora de contêiner, use ambientes virtuais. A suíte conjunta precisa
dos três manifestos no mesmo ambiente e dos bindings em `gateway`, `client` e
`sensor_python`, conforme a preparação abaixo.

### C / POSIX

O sensor climático utiliza compilador compatível com C11, pthreads,
`protobuf-c-compiler` e `libprotobuf-c-dev`. A imagem usa Ubuntu 24.04 e instala a
toolchain durante o build.

### Java 21

O sensor de semáforo é compilado com Eclipse Temurin JDK 21 e executado em JRE 21.
O runtime `protobuf-java-3.25.1.jar` é obtido durante o estágio de build.

### Lua 5.4

O sensor de poste utiliza Lua 5.4, LuaRocks, `lua-protobuf`, `luasocket` e
`luaposix`. O último pacote é necessário para tratar sinais e realizar o encerramento
gracioso.

### Go 1.23

O sensor de estacionamento usa o módulo em
`projeto-sockets/sensor_go/go.mod`. O build gera o pacote Go a partir do contrato
`common/messages.proto`, verifica os checksums com `go mod verify`, executa os
testes com o detector de data races e produz um binário estático para uma imagem
distroless não root.

## 5. Validações antes de uma contribuição

Execute pelo menos:

```bash
cd projeto-sockets
docker compose config --quiet
docker compose build
docker compose up -d --wait --wait-timeout 180
curl --fail http://127.0.0.1:8501/_stcore/health
docker compose down
```

Em uma máquina com Python e `protoc` disponíveis, ative um ambiente virtual e
execute na raiz `projeto-sockets`:

```bash
python -m pip install -r gateway/requirements.txt -r client/requirements.txt -r sensor_python/requirements.txt
protoc -I=common --python_out=gateway common/messages.proto
protoc -I=common --python_out=client common/messages.proto
protoc -I=common --python_out=sensor_python common/messages.proto
python -m compileall -q gateway/main.py gateway/analytics.py client/app.py sensor_python/sensor.py
python -m unittest discover -s tests -v
python -m unittest discover -s sensor_python -p 'test_*.py' -v
```

Os testes usam SQLite temporário e sockets locais, sem exigir o Compose em
execução. Incluem ledger e replay após reinício, rollback, retry e encerramento,
retenção OLAP, correlação de pedidos/respostas, atuação e gráficos do dashboard.
A validação de sintaxe com `compileall` pode ser executada separadamente sem
importar os módulos.

Com GCC disponível, valide o AQI na mesma raiz:

```bash
gcc -std=c11 -Wall -Wextra -Werror sensor_c/test_aqi.c -lm -o test-aqi
./test-aqi
```

No Windows, use `-o test-aqi.exe` e execute `.\test-aqi.exe`. Essa verificação
cobre o cálculo em C; o build completo também exige a toolchain POSIX/Protobuf-C.

Em uma máquina com Go e `protoc-gen-go` disponíveis:

```bash
cd sensor_go
go mod verify
go vet ./...
go test ./...
```

O workflow `.github/workflows/ci.yml` instala os três manifestos Python, gera os
bindings, executa as duas suítes Python e o teste C, e então faz build e smoke test
em pushes da branch principal e pull requests. Para verificar a integração local,
execute também os comandos Compose desta seção.

## 6. Podman no Windows

Se `podman compose` não localizar um provider, instale/configure `podman-compose` e
defina `PODMAN_COMPOSE_PROVIDER=podman-compose`. A partir da raiz do repositório,
também é possível usar o helper:

```powershell
.\projeto-sockets\scripts\podman-compose.ps1 up --build
```

O `projeto-sockets/Dockerfile` contém estágios de fallback para providers que não
respeitam `build.dockerfile`; ele deve permanecer sincronizado com os Dockerfiles de
cada serviço, inclusive `sensor_go/Dockerfile`.

## 7. Problemas comuns

- **Compose não encontra o arquivo:** confirme que o terminal está em
  `projeto-sockets` ou informe `-f projeto-sockets/docker-compose.yml` a partir da
  raiz.
- **Falha ao conectar ao daemon:** inicie Docker Desktop ou a máquina virtual do
  Podman e repita `docker version`/`podman info`.
- **Primeiro build demorado:** as toolchains C, Java, Lua, Go e Protobuf são baixadas na
  primeira execução; builds seguintes aproveitam cache.
- **Porta já ocupada:** altere o serviço conflitante conscientemente no Compose;
  não exponha portas em `0.0.0.0` apenas para contornar o conflito.
- **Descoberta multicast no Podman/Windows:** algumas combinações de rede virtual
  limitam multicast. Verifique os logs do gateway e dos sensores e compare com um
  ambiente Docker/Linux antes de atribuir o erro ao protocolo.
- **Encerramento registra falha de drenagem:** confira disponibilidade e espaço
  do banco, retries de escrita e `TELEMETRY_SHUTDOWN_TIMEOUT_SECS`. Ao aumentar o
  prazo, ajuste também a tolerância de parada do Compose.
