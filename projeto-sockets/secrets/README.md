# Docker Secrets — Smart City

Este diretório guarda os segredos do sistema fora do `docker-compose.yml` em texto puro.
Os arquivos `*.txt` reais são **ignorados pelo git** (`.gitignore`); apenas os
`*.example` são versionados como modelo.

## Como ativar

```bash
cp secrets/aes_secret_key.txt.example     secrets/aes_secret_key.txt
cp secrets/gateway_license.txt.example    secrets/gateway_license.txt
cp secrets/dashboard_password.txt.example secrets/dashboard_password.txt
# edite os valores reais nos .txt copiados

docker compose -f docker-compose.yml -f docker-compose.secrets.yml up --build
```

## Como funciona

Cada serviço lê primeiro `<NOME>_FILE` (caminho do secret em `/run/secrets/...`)
e, se ausente, cai para a env var `<NOME>` — então o sistema continua funcionando
sem o override (modo legado, segredos no compose).

| Secret | Usado por | Variável de arquivo |
|--------|-----------|---------------------|
| `aes_secret_key` | gateway, aggregator_java, aggregator_rust | `AES_SECRET_KEY_FILE` |
| `gateway_license` | gateway | `GATEWAY_LICENSE_KEY_FILE` |
| `dashboard_password` | dashboard | `DASHBOARD_PASSWORD_FILE` |

> ⚠️ A chave AES deve ser **idêntica** entre gateway e agregadores, senão a
> descriptografia da telemetria falha.
