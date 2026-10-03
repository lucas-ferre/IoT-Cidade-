# Como contribuir

Contribuições de correção, documentação e evolução técnica são bem-vindas. Como o
projeto reúne cinco linguagens e diferentes modelos de concorrência, mudanças
pequenas e verificáveis são preferíveis.

## Preparação

É necessário um runtime de contêiner compatível com Docker Compose v2 ou Podman
Compose. A matriz completa de ferramentas está em [SETUP.md](SETUP.md).

```bash
cd projeto-sockets
docker compose config --quiet
docker compose up --build -d --wait
```

O dashboard deve responder em <http://127.0.0.1:8501>. Ao terminar:

```bash
docker compose down
```

## Antes de abrir uma alteração

- mantenha o contrato `common/messages.proto` compatível entre todas as linguagens;
- declare dependências diretas nos manifestos do respectivo serviço;
- não versione bancos, logs, caches ou código gerado pelo Protobuf;
- atualize a documentação quando portas, variáveis ou comportamento mudarem;
- execute `docker compose config --quiet` e o build dos serviços afetados;
- execute `python -m unittest discover -s tests -v` ao alterar o gateway analítico;
- execute `go vet ./...` e `go test ./...` ao alterar o sensor Go;
- para mudanças de integração, confirme que os healthchecks ficam saudáveis.

Commits curtos e descritivos facilitam a revisão. Em pull requests, explique o
problema, a decisão adotada, como o resultado foi validado e eventuais limitações.

Vulnerabilidades não devem ser relatadas em issues públicas; consulte
[SECURITY.md](SECURITY.md).
