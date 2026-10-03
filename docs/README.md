# Documentação técnica

Esta pasta reúne a documentação de engenharia do laboratório **Smart City**. O
[README principal](../README.md) apresenta o projeto; os documentos abaixo
detalham as decisões necessárias para compreender, executar e evoluir o sistema.

## Índice

| Documento | Quando consultar |
|---|---|
| [Arquitetura](architecture.md) | Componentes, fluxos de dados, concorrência, persistência e resiliência |
| [Protocolo](protocol.md) | Contrato Protobuf, portas, framing TCP, validações e compatibilidade |
| [Operações](operations.md) | Execução, configuração, observabilidade, testes e solução de problemas |
| [Configuração do ambiente](../SETUP.md) | Preparação detalhada do Docker/Podman e do ambiente de desenvolvimento |
| [Segurança](../SECURITY.md) | Modelo de ameaça, limitações e recomendações para exposição de rede |
| [Contribuição](../CONTRIBUTING.md) | Fluxo e verificações exigidas para uma contribuição |

## Mapa do código

Os arquivos que definem o comportamento observável do sistema são:

- [`common/messages.proto`](../projeto-sockets/common/messages.proto), contrato
  canônico entre todos os runtimes;
- [`docker-compose.yml`](../projeto-sockets/docker-compose.yml), topologia e
  configuração padrão do laboratório;
- [`gateway/main.py`](../projeto-sockets/gateway/main.py), fronteira de rede,
  persistência, proxy de comandos e consultas;
- [`gateway/analytics.py`](../projeto-sockets/gateway/analytics.py), cálculos puros
  de rollup, seleção de fonte e amostragem;
- [`client/app.py`](../projeto-sockets/client/app.py), dashboard Streamlit;
- [`sensor_go/main.go`](../projeto-sockets/sensor_go/main.go), sensor controlável
  de estacionamento inteligente.

## Fontes de verdade

Para evitar divergências entre documentação e implementação, considere esta
ordem de autoridade:

1. o schema Protobuf define campos, enumerações e compatibilidade no fio;
2. o Compose define serviços, nomes, portas e valores padrão implantados;
3. o código de cada processo define validações e comportamento em execução;
4. estes documentos explicam as decisões e os procedimentos operacionais.

Mudanças no schema, nas portas ou nas variáveis de ambiente devem atualizar a
documentação correspondente na mesma contribuição.

