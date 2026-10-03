# Política e escopo de segurança

## Escopo do projeto

Este repositório é uma simulação educacional de uma cidade inteligente, criada para
estudo de sistemas distribuídos, redes, concorrência e análise de dados. O ambiente
foi projetado para execução local, em uma máquina ou rede de laboratório confiável.

Ele **não deve ser tratado como um sistema pronto para produção** nem receber dados
reais, pessoais, sigilosos ou operacionais.

## Modelo de ameaça e limitações conhecidas

O protocolo atual usa TCP, UDP e Protocol Buffers sem uma camada de identidade
criptográfica. Em particular:

- os hubs autorizam origens por serviço DNS/IPv4 e perfil de dispositivo,
  sem autenticação mútua por credenciais;
- o tráfego não possui TLS, DTLS ou assinatura de mensagens;
- `message_id` e `timestamp` auxiliam a idempotência, mas não comprovam autoria;
- o dashboard Streamlit não possui controle de acesso próprio;
- os hubs descartam participantes desconhecidos e falsificação de IDs a partir
  de um IP diferente do serviço autorizado; isso não protege contra um serviço
  legítimo comprometido, falsificação de origem na infraestrutura ou host Docker
  comprometido;
- o SQLite e o dashboard foram dimensionados para demonstração local, não para
  isolamento multiusuário ou alta disponibilidade.

`gateway_backend` é interna e contém somente gateway e os dois hubs. Sensores,
dashboard e invasor de teste ficam na frontend `smart_city_net`; o gateway não
publica portas e não participa dela. Os hubs são a fronteira de entrada: UDP
5000/5002 para sensores e TCP 5001 para clientes; o dashboard publica HTTP 8501.
O controle segue gateway → hub/5010 → sensor, com endpoint derivado do registro
de origem observado. Um endereço declarado num pacote não autoriza uma conexão.

Por padrão, as portas publicadas pelo Compose são vinculadas a `127.0.0.1`. Não
altere `BIND_ADDRESS` para `0.0.0.0` sem firewall, segmentação de rede e uma análise
explícita dos riscos.

O hub de acesso autoriza `dashboard` por padrão. Clientes TCP adicionais precisam
de uma entrada explícita em `ACCESS_ALLOWED_HOSTS`; publicar uma porta não
autoriza seu uso. A autorização e o transporte atuais usam IPv4. Os hubs limitam
frames, datagramas, conexões, taxa, registro e prazos; essas medidas complementam
a validação e o ledger do gateway, sem comprovar autoria criptográfica.

## Emissor invasor do laboratório

`sensor_invasor` fica fora da inicialização normal e usa o perfil `security-test`.
O processo exige `INTRUDER_ENABLED=1` e envia uma rodada finita de 12 cenários
para os dois hubs explicitamente configurados. Aceita somente destinos IPv4
privados ou loopback; não varre portas, não descobre alvos e não envia comandos
válidos capazes de alterar dispositivos. O limite é de dez rodadas e o intervalo
mínimo de 0,02 segundo.

```bash
cd projeto-sockets
docker compose --profile security-test run --build --rm sensor_invasor
docker compose logs --since=2m hub_sensores hub_acesso
```

Os cenários exercitam identidade desconhecida, falsificação de câmera, Protobuf,
timestamp, valores e tamanhos inválidos, além de pedidos TCP rejeitados. A saída
`sent_not_acknowledged` indica somente emissão UDP; use auditoria/contadores e
testes com observação do backend para comprovar bloqueio.

## Uso seguro no laboratório

- Mantenha o projeto em uma rede confiável e sem encaminhamento público de portas.
- Não reutilize credenciais, chaves ou dados reais em arquivos de configuração.
- Não versione o arquivo `.env`; use `.env.example` apenas como referência.
- Remova o volume `gateway_db` ao encerrar experimentos com dados não confiáveis.
- Revise imagens e dependências antes de qualquer demonstração em infraestrutura
  compartilhada.

## Relato de vulnerabilidades

Evite publicar detalhes exploráveis em uma issue aberta. Prefira o recurso
**Private vulnerability reporting** do GitHub, quando habilitado. Se ele não estiver
disponível, entre em contato com o mantenedor pelo perfil
[lucas-ferre](https://github.com/lucas-ferre) antes de divulgar o problema.

Inclua no relato uma descrição do impacto, passos mínimos de reprodução, versão ou
commit afetado e uma possível mitigação. Não inclua dados de terceiros.

## Requisitos antes de um uso além do laboratório

Uma evolução para um ambiente real exigiria, no mínimo:

- identidade por dispositivo, rotação e revogação de chaves;
- proteção contra replay e integridade dos datagramas;
- TLS/mTLS nos canais TCP e autenticação do dashboard;
- autorização por dispositivo e por tipo de comando;
- ampliar a validação, limites e auditoria existentes com monitoramento e
  políticas para a infraestrutura de destino;
- execução sem privilégios, imagens fixadas por digest e verificação de artefatos;
- banco de dados, retenção e política de privacidade adequados ao domínio.
