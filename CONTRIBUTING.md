# Como contribuir

Contribuições de correção, documentação e evolução técnica são bem-vindas. Como o
projeto reúne sete linguagens de sensores e diferentes modelos de concorrência, mudanças
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
- versione o `sensor_rust/Cargo.lock` e preserve builds/testes `--locked`;
- atualize a documentação quando portas, variáveis ou comportamento mudarem;
- execute `docker compose config --quiet` e o build dos serviços afetados;
- execute `python -m unittest discover -s tests -v` ao alterar o gateway analítico;
- execute as regressões de hubs/invasor ao alterar autorização, framing ou relay;
- execute `go vet ./...` e `go test ./...` ao alterar o sensor Go;
- execute `cargo test --locked` ao alterar Rust;
- execute testes Node e a verificação Protobuf Python independente ao alterar
  o codec TypeScript; Node 22.18+ dispensa dependências npm;
- execute testes de amostras C, Lua e Java ao mudar essas métricas;
- para mudanças de integração, confirme que os healthchecks ficam saudáveis.

O [SETUP](SETUP.md) reúne os comandos por linguagem. Para a validação completa,
rode o smoke test de 66 dispositivos dentro de `dashboard` e, separadamente,
`docker compose --profile security-test run --build --rm sensor_invasor`.
Consulte a auditoria dos hubs para confirmar rejeições UDP; apenas emitir um
datagrama não comprova seu bloqueio.

Mantenha a topologia com `smart_city_net` frontend e `gateway_backend` interna
somente para gateway/hubs. O gateway não publica portas. Mudanças nos tipos ou
IDs de sensor precisam atualizar os perfis DNS/IPv4 dos hubs, o catálogo de
métricas e a documentação. Preserve as unidades; por exemplo, estacionamento
usa `parking_wait_time` em minutos e semáforo usa `average_wait` em segundos.

Commits curtos e descritivos facilitam a revisão. Em pull requests, explique o
problema, a decisão adotada, como o resultado foi validado e eventuais limitações.
Informe quais testes realmente passaram e quais não foram executados por falta
de toolchain ou runtime. A existência de testes no build/CI não equivale a uma
validação concluída na máquina local.

Vulnerabilidades não devem ser relatadas em issues públicas; consulte
[SECURITY.md](SECURITY.md).

A escolha de licença do projeto permanece em aberto; não atribua uma licença
específica aos arquivos do repositório sem decisão do mantenedor.
