# Hubs de entrada

`hub_sensores` recebe descoberta UDP/5002 e telemetria UDP/5000. Ele valida os
envelopes, os horários, os valores das métricas e o perfil do dispositivo antes
de encaminhar ao gateway. A origem observada precisa corresponder ao IPv4 do
serviço autorizado no DNS da rede Docker. Telemetria exige uma descoberta válida
e ainda vigente; o campo `ip_address` anunciado pelo sensor não escolhe o destino
de nenhuma conexão.

O hub armazena o IP observado e a porta de controle original. Ele encaminha as
descobertas controláveis com `control_port=5010`, mantendo o IP original apenas
como informação. O gateway observa o IP do hub e envia a ele os comandos TCP. O
hub consulta seu registro pelo `target_device_id`, conecta exclusivamente ao
endpoint autorizado e confere o ID do ACK e os valores solicitados antes de
devolver sucesso. Alvos desconhecidos ou expirados não abrem uma conexão.

`hub_acesso` recebe TCP/5001, valida os frames de quatro bytes em ordem de rede e
os `ClientRequest`, limita conexões e encaminha pedidos sequenciais ao gateway
em uma conexão persistente. Cada resposta precisa preservar o `message_id` da
requisição. Por padrão, somente o IPv4 do serviço `dashboard` é autorizado;
clientes locais adicionais precisam constar explicitamente em
`ACCESS_ALLOWED_HOSTS`, separados por vírgulas.

| Prefixo | Serviço DNS autorizado | Tipo | Porta original |
|---|---|---:|---:|
| `estacao_` | `sensor_clima` | 3 | 0, sem controle |
| `poste_` | `sensor_posto` | 2 | 5006 |
| `semaforo_` | `sensor_java` | 1 | 5003 |
| `camera_` | `sensor_camera` | 4 | 5004 |
| `parking_` | `sensor_estacionamento` | 6 | 5007 |
| `water_` | `sensor_agua` | 7 | 5008 |
| `waste_` | `sensor_lixeiras` | 8 | 0, sem controle |

Execute `python -m hubs.main sensores` ou `python -m hubs.main acesso`. O comando
`python -m hubs.main healthcheck sensores` verifica TCP/5010; a opção `acesso`
verifica TCP/5001. A conexão de healthcheck não envia nenhuma requisição da
aplicação e não precisa de autorização para encaminhamento.

| Variável | Padrão | Finalidade |
|---|---:|---|
| `GATEWAY_HOST` | `gateway` | Serviço interno de destino; também autoriza a origem de comandos |
| `HUB_BIND_HOST` | `0.0.0.0` | Endereço local de escuta |
| `ACCESS_ALLOWED_HOSTS` | `dashboard` | CSV de serviços/IPs autorizados no hub de acesso |
| `HUB_MAX_CLIENTS` | 128 | Máximo de conexões simultâneas por hub |
| `HUB_REQUEST_TIMEOUT_SECS` | 10 | Prazo de leitura, escrita e conexão |
| `HUB_RATE_PER_SECOND` | 80 | Reposição dos tokens por origem e canal |
| `HUB_RATE_BURST` | 200 | Capacidade da rajada por origem e canal |
| `HUB_DEVICE_TTL_SECS` | 120 | Prazo do registro após a última descoberta aceita |
| `HUB_MAX_DEVICES` | 2048 | Capacidade do registro de dispositivos |
| `HUB_TCP_MAX_FRAME_BYTES` | 1048576 | Limite de corpo de frame TCP |
| `HUB_UDP_MAX_BYTES` | 16384 | Limite de datagrama UDP |
| `HUB_MESSAGE_MAX_AGE_SECS` | 86400 | Idade máxima de envelopes |
| `HUB_MESSAGE_MAX_FUTURE_SKEW_SECS` | 300 | Tolerância de relógio futuro |
| `HUB_DNS_REFRESH_SECS` | 15 | Intervalo de renovação das origens autorizadas |

Comandos têm limites adicionais de 300 segundos de idade e 30 segundos no futuro,
compatíveis com os atuadores. Falhas de DNS deixam de autorizar endereços antigos
depois de três intervalos de renovação. As listas de dispositivos, conexões e
origens do limitador são limitadas para manter o uso de memória previsível.

Os logs JSON contêm `hub_id`, `event`, `reason`, `source_ip`, `device_id` e `count`.
As cinco primeiras ocorrências de cada motivo e cada centésima ocorrência são
registradas; os contadores incluem todos os pacotes e são publicados a cada
renovação DNS. Os heartbeats dos sensores renovam seus registros nas duas redes
sem depender do multicast emitido pelo gateway na rede interna.

Essa autorização usa origem de rede e perfil do serviço no laboratório isolado.
O protocolo atual mantém TCP/UDP Protobuf sem assinatura criptográfica.
