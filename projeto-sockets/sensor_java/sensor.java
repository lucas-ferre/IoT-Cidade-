import java.io.*;
import java.net.*;
import java.time.Instant;
import smartcity.Messages; // Pacote gerado pelo compilador protoc

public class sensor {
    // ====================================================================
    // CONFIGURAÇÕES DE REDE E IDENTIFICAÇÃO
    // ====================================================================
    private static final java.util.Random RNG = new java.util.Random();
    private static final String[][] SECTORS = {
        {"Pici", "pici"},
        {"Benfica", "benfica"},
        {"Porangabussu", "porangabussu"},
        {"Labomar", "labomar"}
    };
    private static final int DEVICE_COUNT = Math.max(1, Integer.parseInt(System.getenv().getOrDefault("JAVA_DEVICE_COUNT", String.valueOf(SECTORS.length))));
    private static final java.util.Map<String, DeviceState> DEVICES = new java.util.concurrent.ConcurrentHashMap<>();
    private static final java.util.List<String> DEVICE_ORDER = new java.util.concurrent.CopyOnWriteArrayList<>();
    private static final String DEFAULT_DEVICE_ID;
    private static volatile String GATEWAY_HOST = "gateway";
    private static volatile double BEST_AGGREGATOR_SCORE = 999999.0;
    private static final String DEVICE_HOSTNAME;
    static {
        String h = "sensor_semaforo";
        try {
            String gw = System.getenv().getOrDefault("GATEWAY_HOST", "gateway");
            InetAddress gwAddress = InetAddress.getByName(gw);
            try (DatagramSocket socket = new DatagramSocket()) {
                socket.connect(gwAddress, 5000);
                h = socket.getLocalAddress().getHostAddress();
            }
        } catch (Exception ignored) {}
        DEVICE_HOSTNAME = h;
    }

    private static final int UDP_MAX_RETRIES = 3;

    private static final DatagramSocket DISC_SOCKET;
    static {
        try {
            DISC_SOCKET = new DatagramSocket();
        } catch (SocketException e) {
            throw new ExceptionInInitializerError(
                "Falha ao criar socket UDP de descoberta: " + e.getMessage()
            );
        }
    }

    // Portas segregadas para multiplexação espacial UDP
    private static int GATEWAY_TELEMETRY_PORT = 5000;
    private static final int GATEWAY_DISCOVERY_PORT = 5002;
    private static final int AUTH_TCP_PORT = 5007;
    private static final String SENSOR_LICENSE_PART = System.getenv().getOrDefault("SENSOR_LICENSE_PART", "V1-FULL");
    private static final String SENSOR_HEX_CODE = "0A";

    private static final int CONTROL_TCP_PORT = 5003;
    private static final String MULTICAST_GROUP = "239.0.0.1";
    private static final int MULTICAST_PORT = 5005;
    private static final long RETRY_BASE_DELAY_MS = 200L;
    private static final long RETRY_MAX_DELAY_MS = 1500L;
    private static final long TELEMETRY_JITTER_MS = 350L;
    private static final long DISCOVERY_PROBE_JITTER_MS = 2_000L;
    private static final long HEARTBEAT_INTERVAL_MS = envSecondsToMillis(
        "SENSOR_HEARTBEAT_INTERVAL_SECS", 10.0, 1_000L
    );
    private static final long HEARTBEAT_JITTER_MS = envSecondsToMillis(
        "SENSOR_HEARTBEAT_JITTER_SECS", 2.0, 0L
    );
    private static final int MAX_TCP_FRAME_BYTES = 1024 * 1024;
    private static final long MANUAL_OVERRIDE_MS = 30_000L;
    private static final long THRESHOLD_SCAN_INTERVAL_MS = 1_000L;
    private static final long THRESHOLD_EVENT_COOLDOWN_MS = 3_000L;
    private static final int TRAFFIC_QUEUE_THRESHOLD = Integer.parseInt(
        System.getenv().getOrDefault("TRAFFIC_QUEUE_THRESHOLD", "35")
    );

    // Guarda anti-replay compartilhada por todas as conexões de controle.
    private static final ControlCrypto.ReplayGuard REPLAY_GUARD = new ControlCrypto.ReplayGuard();

    private static class DeviceState {
        final String deviceId;
        final String sector;
        final int coordX;
        final int coordY;
        volatile int currentStatus = Messages.DeviceStatus.STATUS_ON_VALUE;
        volatile int frequencySecs = 5;
        volatile long nextSendAtMillis = 0L;
        volatile long nextThresholdCheckAtMillis = 0L;
        volatile long lastThresholdSendAtMillis = 0L;
        volatile long manualUntilMillis = 0L;

        DeviceState(String deviceId, String sector, int coordX, int coordY) {
            this.deviceId = deviceId;
            this.sector = sector;
            this.coordX = coordX;
            this.coordY = coordY;
        }
    }

    private static class UdpSendResult {
        final boolean success;
        final String errorMessage;

        private UdpSendResult(boolean success, String errorMessage) {
            this.success = success;
            this.errorMessage = errorMessage;
        }

        static UdpSendResult ok() {
            return new UdpSendResult(true, "");
        }

        static UdpSendResult failed(String errorMessage) {
            return new UdpSendResult(false, errorMessage);
        }
    }

    static {
        for (int i = 0; i < DEVICE_COUNT; i++) {
            int sectorIdx = i % SECTORS.length;
            int sectorOrdinal = (i / SECTORS.length) + 1;
            String sectorSlug = SECTORS[sectorIdx][1];
            String deviceId = "semaforo_" + sectorSlug + "_"
                + String.format("%02d", sectorOrdinal);
            
            int cx = 0, cy = 0;
            if (sectorSlug.equals("pici")) {
                cx = RNG.nextInt(41); cy = RNG.nextInt(61);
            } else if (sectorSlug.equals("benfica")) {
                cx = 50 + RNG.nextInt(41); cy = RNG.nextInt(31);
            } else if (sectorSlug.equals("porangabussu")) {
                cx = 60 + RNG.nextInt(41); cy = 50 + RNG.nextInt(41);
            } else { // labomar
                cx = RNG.nextInt(31); cy = 70 + RNG.nextInt(31);
            }

            DeviceState device = new DeviceState(deviceId, SECTORS[sectorIdx][0], cx, cy);
            DEVICES.put(deviceId, device);
            DEVICE_ORDER.add(deviceId);
        }
        DEFAULT_DEVICE_ID = DEVICE_ORDER.get(0);
    }

    public static void main(String[] args) {
        System.out.println("============================================================");
        System.out.println("[Java] Frota de semáforos iniciada (Arquitetura Multiplexada).");
        for (String deviceId : DEVICE_ORDER) {
            DeviceState device = DEVICES.get(deviceId);
            System.out.println("       Dispositivo=" + device.deviceId + " | Setor=" + device.sector);
        }
        System.out.println("============================================================");

        // 1. Alocação da Thread de Recuperação via canal Multicast
        new Thread(sensor::startMulticastListener).start();

        System.out.println("[Java] Aguardando broadcast de AggregatorLoad para descobrir IP real...");
        String initialGateway = System.getenv().getOrDefault("GATEWAY_HOST", "gateway");
        while (GATEWAY_HOST.equals("gateway") || GATEWAY_HOST.equals(initialGateway)) {
            try { Thread.sleep(100); } catch (Exception e) {}
        }

        // 2. Injeção do Handshake inicial na rede de descoberta
        sendDiscovery();

        GATEWAY_TELEMETRY_PORT = authenticateWithGateway();
        try { Thread.sleep(2000); } catch (Exception e) {}

        // 3. Alocação da Thread de Controle TCP (Atuação remota)
        new Thread(sensor::startTcpServer).start();

        // 4. Renovação periódica explícita de presença no Gateway
        new Thread(sensor::startHeartbeatLoop).start();

        // 4.5 Console IDLE embutido (opt-in via SENSOR_IDLE_CONSOLE). Conecta-se ao
        // próprio servidor de controle local; requer terminal anexado (stdin_open + tty).
        if (idleConsoleEnabled()) {
            Thread idle = new Thread(() -> Console.runConsole("127.0.0.1", CONTROL_TCP_PORT));
            idle.setDaemon(true);
            idle.start();
            System.out.println("[Java:IDLE] Console embutido ativo (use 'docker attach').");
        }

        // 5. Bloqueio da Thread Principal no Loop de Telemetria UDP
        runTelemetryLoop();
    }

    // ====================================================================
    // ROTINAS DE PROTOCOLO E COMUNICAÇÃO
    // ====================================================================

    private static boolean idleConsoleEnabled() {
        String raw = System.getenv().getOrDefault("SENSOR_IDLE_CONSOLE", "").trim().toLowerCase();
        return raw.equals("1") || raw.equals("true") || raw.equals("yes") || raw.equals("on");
    }

    private static long retryDelayMillis(int attempt) {
        long temp = RETRY_BASE_DELAY_MS;
        for (int i = 0; i < attempt; i++) {
            temp *= 2;
            if (temp >= RETRY_MAX_DELAY_MS) {
                temp = RETRY_MAX_DELAY_MS;
                break;
            }
        }
        return RNG.nextInt((int) temp + 1);
    }

    private static long telemetryDelayMillis(int frequencySecs) {
        return (frequencySecs * 1000L) + RNG.nextInt((int) TELEMETRY_JITTER_MS + 1);
    }

    private static long envSecondsToMillis(String name, double defaultValue, long minMillis) {
        String raw = System.getenv().getOrDefault(name, Double.toString(defaultValue));
        try {
            return Math.max(minMillis, Math.round(Double.parseDouble(raw) * 1000.0));
        } catch (NumberFormatException e) {
            System.err.println("[Java:Config] Valor inválido para " + name + "='" + raw
                + "'. Usando padrão " + defaultValue + "s.");
            return Math.max(minMillis, Math.round(defaultValue * 1000.0));
        }
    }

    private static long heartbeatDelayMillis() {
        long jitter = HEARTBEAT_JITTER_MS <= 0L
            ? 0L
            : (long) (RNG.nextDouble() * ((double) HEARTBEAT_JITTER_MS + 1.0));
        return HEARTBEAT_INTERVAL_MS + jitter;
    }

    private static int randomDeviceStatus() {
        int roll = RNG.nextInt(100);
        if (roll < 78) return Messages.DeviceStatus.STATUS_ON_VALUE;
        if (roll < 90) return Messages.DeviceStatus.STATUS_OFF_VALUE;
        return Messages.DeviceStatus.STATUS_ERROR_VALUE;
    }

    private static int sampleQueueLength() {
        return 5 + RNG.nextInt(46);
    }

    private static String queueThresholdReason(int queueLength) {
        if (queueLength >= TRAFFIC_QUEUE_THRESHOLD) {
            return "queue_length=" + queueLength + " >= " + TRAFFIC_QUEUE_THRESHOLD;
        }
        return null;
    }

    private static String cleanNetworkError(Exception e) {
        String message = e.getMessage();
        if (message == null || message.isBlank()) {
            return e.getClass().getSimpleName();
        }
        String gatewayPrefix = GATEWAY_HOST + ": ";
        if (message.startsWith(gatewayPrefix)) {
            return message.substring(gatewayPrefix.length());
        }
        return message;
    }

    private static String formatDelaySeconds(long delayMillis) {
        return String.format(java.util.Locale.ROOT, "%.2fs", delayMillis / 1000.0);
    }

    private static boolean waitDiscoveryProbeJitter() {
        long delay = RNG.nextInt((int) DISCOVERY_PROBE_JITTER_MS + 1);
        System.out.println("[Java:Multicast] Jitter de redescoberta: " + delay + " ms.");
        try {
            Thread.sleep(delay);
            return true;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static UdpSendResult sendUdpWithRetry(DatagramSocket socket, byte[] buf, int port) {
        String lastError = "falha desconhecida";

        for (int attempt = 0; attempt < UDP_MAX_RETRIES; attempt++) {
            try {
                InetAddress gatewayAddr = InetAddress.getByName(GATEWAY_HOST);
                socket.send(new DatagramPacket(buf, buf.length, gatewayAddr, port));
                return UdpSendResult.ok();
            } catch (Exception e) {
                lastError = cleanNetworkError(e);

                if (attempt == UDP_MAX_RETRIES - 1) {
                    System.err.println("[sensor_semaforo] | [Sensor Java:Retry] Falha UDP porta "
                        + port + " (tentativa " + (attempt + 1) + "/" + UDP_MAX_RETRIES
                        + "): " + lastError + ". Todas as tentativas esgotadas.");
                    break;
                }

                long delay = retryDelayMillis(attempt);
                System.err.println("[sensor_semaforo] | [Sensor Java:Retry] Falha UDP porta "
                    + port + " (tentativa " + (attempt + 1) + "/" + UDP_MAX_RETRIES
                    + "): " + lastError + ". Retry em " + formatDelaySeconds(delay) + ".");

                try {
                    Thread.sleep(delay);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return UdpSendResult.failed(lastError);
                }
            }
        }

        return UdpSendResult.failed(lastError);
    }

    /** Empacota e despacha o descritor de topologia para a porta 5002 */
    private static synchronized void sendDiscovery() {
        for (String deviceId : DEVICE_ORDER) {
            sendDiscovery(deviceId);
        }
    }

    private static synchronized void sendDiscovery(String deviceId) {
        DeviceState device = DEVICES.get(deviceId);
        if (device == null) return;

        // Reutiliza DISC_SOCKET estático — sem criação/destruição de socket por chamada
        try {
            Messages.DiscoveryResponse disc = Messages.DiscoveryResponse.newBuilder()
                .setMessageId("DISC-" + device.deviceId + "-" + System.currentTimeMillis())
                .setTimestamp(Instant.now().getEpochSecond())
                .setDeviceId(device.deviceId)
                .setType(Messages.DeviceType.DEVICE_TYPE_TRAFFIC_LIGHT)
                .setIpAddress(DEVICE_HOSTNAME)
                .setControlPort(CONTROL_TCP_PORT)
                .setIsControllable(true)
                .setInitialStatus(Messages.DeviceStatus.forNumber(device.currentStatus))
                .setCoordX(device.coordX)
                .setCoordY(device.coordY)
                .build();

            byte[] buf = disc.toByteArray();

            // Roteamento exclusivo para o pipeline de Descoberta
            UdpSendResult sendResult = sendUdpWithRetry(DISC_SOCKET, buf, GATEWAY_DISCOVERY_PORT);
            if (sendResult.success) {
                System.out.println("[Java:Descoberta] Dispositivo=" + device.deviceId
                    + " | Setor=" + device.sector
                    + " | Status=" + Messages.DeviceStatus.forNumber(device.currentStatus)
                    + " | Handshake de topologia emitido com sucesso.");
            } else {
                System.err.println("[sensor_semaforo] | [Sensor Java:Erro] Falha ao enviar descoberta de "
                    + device.deviceId + ": " + sendResult.errorMessage);
            }
        } catch (Exception e) {
            System.err.println("[sensor_semaforo] | [Sensor Java:Erro] Falha ao preparar descoberta de "
                + device.deviceId + ": " + cleanNetworkError(e));
        }
    }

    private static int authenticateWithGateway() {
        System.out.println("[sensor_semaforo] | [Auth] Iniciando autenticacao TCP com Gateway (" + GATEWAY_HOST + ":" + AUTH_TCP_PORT + ")...");
        try { Thread.sleep(1000); } catch (Exception e) {}
        
        try {
            InetAddress gwAddress = InetAddress.getByName(GATEWAY_HOST);
            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress(gwAddress, AUTH_TCP_PORT), 10000);
                DataOutputStream out = new DataOutputStream(s.getOutputStream());
                DataInputStream in = new DataInputStream(s.getInputStream());
                
                Messages.AuthRequest req = Messages.AuthRequest.newBuilder()
                    .setDeviceId(DEFAULT_DEVICE_ID)
                    .setType(Messages.DeviceType.DEVICE_TYPE_TRAFFIC_LIGHT)
                    .setLicenseKeyPart(SENSOR_LICENSE_PART)
                    .setHexServiceCode(SENSOR_HEX_CODE)
                    .build();
                    
                byte[] payload = req.toByteArray();
                out.writeInt(payload.length);
                out.write(payload);
                
                int respLen = in.readInt();
                byte[] respPayload = new byte[respLen];
                in.readFully(respPayload);
                
                Messages.AuthResponse resp = Messages.AuthResponse.parseFrom(respPayload);
                if (!resp.getSuccess()) {
                    System.err.println("[sensor_semaforo] | [Auth] FALHA na validacao: " + resp.getMessage());
                    System.exit(1);
                }
                
                System.out.println("[Auth] Gateway encontrado! Tipo: sensor_semaforo, Chave: '" + SENSOR_LICENSE_PART + "-" + SENSOR_HEX_CODE + "'. Validação: SUCESSO. Porta alocada e conectada: " + resp.getAssignedPort() + ".");
                return resp.getAssignedPort();
            }
        } catch (Exception e) {
            System.err.println("[sensor_semaforo] | [Auth] Erro ao conectar/autenticar com Gateway: " + e.getMessage());
            System.exit(1);
        }
        return GATEWAY_TELEMETRY_PORT;
    }

    private static void startHeartbeatLoop() {
        while (true) {
            try {
                Thread.sleep(heartbeatDelayMillis());
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            }

            System.out.println("[Java:Heartbeat] Renovando presença da frota via DiscoveryResponse.");
            sendDiscovery();
        }
    }

    /** Escreve um frame de resposta, cifrando-o quando CONTROL_SECURE=1. */
    private static void writeFrame(DataOutputStream out, byte[] msg) throws Exception {
        byte[] frame = ControlCrypto.SECURE ? ControlCrypto.wrap(msg) : msg;
        out.writeInt(frame.length);
        out.write(frame);
    }

    /** Instancia o servidor TCP implementando Length-Prefix Framing */
    private static void startTcpServer() {
        try (ServerSocket server = new ServerSocket(CONTROL_TCP_PORT)) {
            System.out.println("[Java:TCP] Interface de atuação aguardando conexões na porta " + CONTROL_TCP_PORT);

            while (true) {
                try (Socket s = server.accept();
                     DataInputStream in = new DataInputStream(s.getInputStream());
                     DataOutputStream out = new DataOutputStream(s.getOutputStream())) {

                    // Framing: Extração estrita do prefixo de 4 bytes (Big-Endian nativo do Java)
                    int len = in.readInt();
                    if (len <= 0 || len > MAX_TCP_FRAME_BYTES) {
                        throw new IOException("Frame TCP inválido: " + len + " bytes");
                    }
                    byte[] payload = new byte[len];
                    in.readFully(payload);

                    if (ControlCrypto.SECURE) {
                        payload = ControlCrypto.unwrap(payload);
                    }

                    // Desserialização segura a partir do tamanho extraído
                    Messages.ConfigCommand cmd = Messages.ConfigCommand.parseFrom(payload);
                    System.out.println("[Java:TCP] Comando interceptado. ID: " + cmd.getCommandId());

                    if (ControlCrypto.SECURE) {
                        String reject = REPLAY_GUARD.check(cmd.getCommandId(), cmd.getTimestamp());
                        if (reject != null) {
                            System.err.println("[Java:TCP] Comando rejeitado (anti-replay): " + reject);
                            Messages.ConfigResponse rej = Messages.ConfigResponse.newBuilder()
                                .setCommandId(cmd.getCommandId())
                                .setSuccess(false)
                                .setMessage("Comando rejeitado (anti-replay): " + reject)
                                .build();
                            writeFrame(out, rej.toByteArray());
                            continue;
                        }
                    }

                    // Mutações de estado em memória volátil
                    String targetDeviceId = cmd.getTargetDeviceId().isBlank()
                        ? DEFAULT_DEVICE_ID
                        : cmd.getTargetDeviceId();
                    DeviceState target = DEVICES.get(targetDeviceId);
                    if (target == null) {
                        Messages.ConfigResponse resp = Messages.ConfigResponse.newBuilder()
                            .setCommandId(cmd.getCommandId())
                            .setSuccess(false)
                            .setMessage("Dispositivo alvo desconhecido no semáforo Java.")
                            .build();

                        writeFrame(out, resp.toByteArray());
                        continue;
                    }

                    synchronized (target) {
                        if (cmd.getUpdateStatus()) {
                            target.currentStatus = cmd.getTargetStatus().getNumber();
                            target.manualUntilMillis = System.currentTimeMillis() + MANUAL_OVERRIDE_MS;
                        }
                    }
                    // Log fora do bloco synchronized — I/O com lock adquirido é má prática.
                    if (cmd.getUpdateStatus()) {
                        System.out.println("[Java:Atuação] Dispositivo=" + target.deviceId
                            + " | Setor=" + target.sector
                            + " | Transição de estado para: " + target.currentStatus);
                    }
                    if (cmd.getUpdateFrequency()) {
                        // frequencySecs é volatile — write simples, não precisa de synchronized
                        target.frequencySecs = cmd.getNewFrequencySecs();
                        System.out.println("[Java:Atuação] Dispositivo=" + target.deviceId
                            + " | Setor=" + target.sector
                            + " | Nova frequência de amostragem: " + target.frequencySecs + "s");
                    }
                    target.nextSendAtMillis = 0L;

                    // Construção do Frame de Confirmação (ACK)
                    Messages.ConfigResponse resp = Messages.ConfigResponse.newBuilder()
                        .setCommandId(cmd.getCommandId())
                        .setSuccess(true)
                        .setMessage("Semáforo Java " + target.deviceId + " reconfigurado com sucesso.")
                        .setUpdatedStatus(Messages.DeviceStatus.forNumber(target.currentStatus))
                        .setUpdatedFrequencySecs(target.frequencySecs)
                        .build();

                    // Aplicação do Framing na resposta (cifrada se CONTROL_SECURE)
                    writeFrame(out, resp.toByteArray());
                    sendDiscovery(target.deviceId);
                } catch (Exception e) {
                    // SocketException("interrupted") é a forma como operações de socket
                    // sinalizam interrupção do thread — não é identificável como InterruptedException.
                    // Verificar o flag antes de logar e continuar o loop de conexões.
                    if (Thread.currentThread().isInterrupted()) {
                        Thread.currentThread().interrupt();
                        System.out.println("[Java:TCP] Thread interrompida — encerrando servidor TCP.");
                        return;
                    }
                    System.err.println("[Java:Erro] Falha no pipeline TCP: " + e.getMessage());
                }
            }
        } catch (Exception e) {
            // Interrupção durante ServerSocket.accept() ou na criação do servidor.
            if (Thread.currentThread().isInterrupted()) {
                Thread.currentThread().interrupt();
                return;
            }
            e.printStackTrace();
        }
    }

    /** Intercepta Probes de Disaster Recovery do Hub Coordenador */
    private static void startMulticastListener() {
        try (MulticastSocket mc = new MulticastSocket(MULTICAST_PORT)) {
            InetAddress group = InetAddress.getByName(MULTICAST_GROUP);

            NetworkInterface ni = null;
            java.util.Enumeration<NetworkInterface> ifaces = NetworkInterface.getNetworkInterfaces();
            while (ifaces != null && ifaces.hasMoreElements()) {
                NetworkInterface iface = ifaces.nextElement();
                if (iface.isUp() && !iface.isLoopback() && iface.supportsMulticast()) {
                    ni = iface;
                    break;
                }
            }

            // Passa ni (pode ser null → JVM usa interface padrão do sistema)
            mc.joinGroup(new InetSocketAddress(group, MULTICAST_PORT), ni);
            System.out.println("[Java:Multicast] Inscrito no grupo de resiliência "
                + MULTICAST_GROUP + ":" + MULTICAST_PORT
                + (ni != null ? " via interface " + ni.getName() : " (interface padrão do sistema)"));

            byte[] buf = new byte[256];
            while (true) {
                DatagramPacket p = new DatagramPacket(buf, buf.length);
                mc.receive(p);

                try {
                    byte[] pureData = new byte[p.getLength()];
                    System.arraycopy(p.getData(), 0, pureData, 0, p.getLength());
                    Messages.AggregatorLoad loadMsg = Messages.AggregatorLoad.parseFrom(pureData);
                    
                    if (loadMsg != null && "GATEWAY_PROBE".equals(loadMsg.getAggregatorId())) {
                        System.out.println("[Java:Multicast] Probe de recuperação detectado. Re-sincronizando topologia com jitter!");
                        if (!waitDiscoveryProbeJitter()) {
                            continue;
                        }
                        sendDiscovery();
                    } else if (loadMsg != null && !loadMsg.getIpAddress().isEmpty()) {
                        double score = (loadMsg.getCpuLoad() * 0.4) + (loadMsg.getQueueSize() * 0.6);
                        if (score < BEST_AGGREGATOR_SCORE || GATEWAY_HOST.equals(loadMsg.getIpAddress())) {
                            if (!GATEWAY_HOST.equals(loadMsg.getIpAddress())) {
                                System.out.printf(java.util.Locale.US, "[Sensor Java:LoadBalancer] Rota alterada para %s (Score: %.2f -> %.2f)\n", loadMsg.getAggregatorId(), BEST_AGGREGATOR_SCORE, score);
                                GATEWAY_HOST = loadMsg.getIpAddress();
                            }
                            BEST_AGGREGATOR_SCORE = score;
                        }
                    }
                } catch (Exception ex) {
                    // Ignora pacotes corrompidos
                }
            }
        } catch (Exception e) {
            // mc.receive() lança SocketException (não InterruptedException) quando o
            // thread é interrompido. Restaurar o flag e sair limpo em vez de printar stacktrace.
            if (Thread.currentThread().isInterrupted()) {
                Thread.currentThread().interrupt();
                return;
            }
            e.printStackTrace();
        }
    }

    /** Motor contínuo de amostragem e despacho de DataPayload */
    private static void sendTelemetryPayload(
        DatagramSocket socket,
        DeviceState device,
        String triggerReason,
        Integer queueLengthOverride
    ) {
        long now = Instant.now().getEpochSecond();

        // Composição de ID único para idempotência no Gateway
        String msgId = device.deviceId + "-" + now + "-" + java.util.UUID.randomUUID().toString().substring(0, 5);

        Messages.DataPayload.Builder builder = Messages.DataPayload.newBuilder()
            .setMessageId(msgId)
            .setTimestamp(now)
            .setDeviceId(device.deviceId)
            .setCurrentStatus(Messages.DeviceStatus.forNumber(device.currentStatus))
            .setCoordX(device.coordX)
            .setCoordY(device.coordY);

        Integer queueLength = queueLengthOverride;
        if (device.currentStatus == Messages.DeviceStatus.STATUS_ON_VALUE) {
            if (queueLength == null) {
                queueLength = sampleQueueLength();
            }
            builder.addMetrics(Messages.Metric.newBuilder().setName("state").setValue(1).setUnit("code"));
            builder.addMetrics(Messages.Metric.newBuilder().setName("queue_length").setValue(queueLength).setUnit("vehicles"));
        }

        Messages.DataPayload p = builder.build();
        byte[] b = p.toByteArray();

        // Roteamento estrito para o pipeline de Telemetria contínua
        UdpSendResult sendResult = sendUdpWithRetry(socket, b, GATEWAY_TELEMETRY_PORT);
        if (sendResult.success) {
            String eventLabel = triggerReason == null ? "Telemetria injetada" : "Evento por limiar";
            System.out.println("[Java:UDP] " + eventLabel
                + " | Dispositivo=" + device.deviceId
                + " | Setor=" + device.sector
                + " | ID=" + msgId
                + " | Status=" + Messages.DeviceStatus.forNumber(device.currentStatus)
                + (queueLength == null ? "" : " | Fila=" + queueLength + " veiculos")
                + (triggerReason == null ? "" : " | Limiar=" + triggerReason));
        } else {
            System.err.println("[sensor_semaforo] | [Sensor Java:Erro] Falha ao enviar telemetria de "
                + device.deviceId + ": " + sendResult.errorMessage);
        }
    }

    private static void pollThresholdEvent(DatagramSocket socket, DeviceState device, long nowMillis) {
        if (nowMillis < device.nextThresholdCheckAtMillis) {
            return;
        }
        device.nextThresholdCheckAtMillis = nowMillis + THRESHOLD_SCAN_INTERVAL_MS;

        if (
            device.currentStatus != Messages.DeviceStatus.STATUS_ON_VALUE
            || nowMillis - device.lastThresholdSendAtMillis < THRESHOLD_EVENT_COOLDOWN_MS
        ) {
            return;
        }

        int queueLength = sampleQueueLength();
        String triggerReason = queueThresholdReason(queueLength);
        if (triggerReason != null) {
            device.lastThresholdSendAtMillis = nowMillis;
            sendTelemetryPayload(socket, device, triggerReason, queueLength);
        }
    }

    private static void runTelemetryLoop() {
        try (DatagramSocket socket = new DatagramSocket()) {
            while (true) {
                long nowMillis = System.currentTimeMillis();
                for (String deviceId : DEVICE_ORDER) {
                    DeviceState device = DEVICES.get(deviceId);
                    if (device == null) {
                        continue;
                    }

                    pollThresholdEvent(socket, device, nowMillis);

                    if (nowMillis < device.nextSendAtMillis) {
                        continue;
                    }

                    // Seção crítica: check(manualUntilMillis) e act(currentStatus) devem
                    // ser atômicos. Sem synchronized, o TCP thread pode aplicar um
                    // STATUS_OFF + manualUntilMillis=futuro entre o check e o write aqui,
                    // e o loop sobrescreve o comando com um status aleatório.
                    synchronized (device) {
                        if (nowMillis >= device.manualUntilMillis) {
                            device.currentStatus = randomDeviceStatus();
                        }
                    }
                    device.nextSendAtMillis = nowMillis + telemetryDelayMillis(device.frequencySecs);

                    Integer queueLength = null;
                    if (device.currentStatus == Messages.DeviceStatus.STATUS_ON_VALUE) {
                        queueLength = sampleQueueLength();
                        if (queueThresholdReason(queueLength) != null) {
                            device.lastThresholdSendAtMillis = nowMillis;
                        }
                    }

                    sendTelemetryPayload(socket, device, null, queueLength);
                }

                // Suspensão curta: cada dispositivo gerencia sua própria janela de amostragem.
                Thread.sleep(200L);
            }
        } catch (InterruptedException ie) {
            // Thread.sleep(200L) lança InterruptedException quando o thread é
            // interrompido. Capturar separadamente de Exception para restaurar o flag
            // e encerrar o loop limpo, sem printar stacktrace.
            Thread.currentThread().interrupt();
            System.out.println("[Java:UDP] Thread de telemetria interrompida — encerrando loop.");
        } catch (Exception e) {
            // Verificar interrupção também aqui: operações de socket podem lançar
            // SocketException("interrupted") em vez de InterruptedException.
            if (Thread.currentThread().isInterrupted()) {
                Thread.currentThread().interrupt();
                return;
            }
            e.printStackTrace();
        }
    }
}
