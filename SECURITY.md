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

- sensores, gateway e dashboard não se autenticam mutuamente;
- o tráfego não possui TLS, DTLS ou assinatura de mensagens;
- `message_id` e `timestamp` auxiliam a idempotência, mas não comprovam autoria;
- o dashboard Streamlit não possui controle de acesso próprio;
- um participante com acesso à rede do laboratório pode forjar telemetria,
  descoberta ou comandos;
- o SQLite e o dashboard foram dimensionados para demonstração local, não para
  isolamento multiusuário ou alta disponibilidade.

Por padrão, as portas publicadas pelo Compose são vinculadas a `127.0.0.1`. Não
altere `BIND_ADDRESS` para `0.0.0.0` sem firewall, segmentação de rede e uma análise
explícita dos riscos.

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
- validação estrita, rate limiting, auditoria e observabilidade;
- execução sem privilégios, imagens fixadas por digest e verificação de artefatos;
- banco de dados, retenção e política de privacidade adequados ao domínio.

