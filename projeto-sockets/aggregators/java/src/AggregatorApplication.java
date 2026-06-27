import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.params.XAddParams;
import smartcity.Messages.AggregatorLoad;

import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentHashMap;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.SecureRandom;
import java.nio.ByteBuffer;

public class AggregatorApplication {

    public static final int TELEMETRY_PORT = 5000;
    public static final int DISCOVERY_PORT = 5002;
    public static final ConcurrentHashMap<Integer, io.netty.channel.Channel> activePorts = new ConcurrentHashMap<>();

    /** Lê um segredo de <NAME>_FILE (Docker secret) caindo para a env var <NAME>. */
    static String readSecret(String name, String def) {
        String filePath = System.getenv(name + "_FILE");
        if (filePath != null && !filePath.isEmpty()) {
            try {
                return new String(
                    java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(filePath)),
                    java.nio.charset.StandardCharsets.UTF_8
                ).trim();
            } catch (Exception e) {
                System.err.println("Falha ao ler " + name + "_FILE: " + e.getMessage());
            }
        }
        return System.getenv().getOrDefault(name, def);
    }

    public static void main(String[] args) throws InterruptedException {
        System.out.println("Iniciando Agregador de Alta Performance Java (Netty)...");

        ConcurrentLinkedQueue<byte[]> telemetryQueue = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<byte[]> discoveryQueue = new ConcurrentLinkedQueue<>();

        RedisWriterWorker worker = new RedisWriterWorker(telemetryQueue, discoveryQueue);
        worker.start();

        MulticastAnnouncer announcer = new MulticastAnnouncer(telemetryQueue);
        announcer.start();

        EventLoopGroup group = new NioEventLoopGroup(4);
        
        ControlCommandWorker controlWorker = new ControlCommandWorker(group, telemetryQueue);
        controlWorker.start();
        try {
            Bootstrap telemetryBootstrap = new Bootstrap();
            telemetryBootstrap.group(group)
                    .channel(NioDatagramChannel.class)
                    .option(ChannelOption.SO_RCVBUF, 1024 * 1024)
                    .handler(new ChannelInitializer<NioDatagramChannel>() {
                        @Override
                        protected void initChannel(NioDatagramChannel ch) {
                            ch.pipeline().addLast(new UdpPacketHandler(telemetryQueue));
                        }
                    });

            Bootstrap discoveryBootstrap = new Bootstrap();
            discoveryBootstrap.group(group)
                    .channel(NioDatagramChannel.class)
                    .option(ChannelOption.SO_RCVBUF, 1024 * 1024)
                    .handler(new ChannelInitializer<NioDatagramChannel>() {
                        @Override
                        protected void initChannel(NioDatagramChannel ch) {
                            ch.pipeline().addLast(new UdpPacketHandler(discoveryQueue));
                        }
                    });

            ChannelFuture futureTelemetry = telemetryBootstrap.bind("0.0.0.0", TELEMETRY_PORT).sync();
            activePorts.put(TELEMETRY_PORT, futureTelemetry.channel());
            ChannelFuture futureDiscovery = discoveryBootstrap.bind("0.0.0.0", DISCOVERY_PORT).sync();
            activePorts.put(DISCOVERY_PORT, futureDiscovery.channel());

            System.out.println("Agregador Netty ouvindo UDP Telemetria (5000) e Discovery (5002).");

            futureTelemetry.channel().closeFuture().sync();
            futureDiscovery.channel().closeFuture().sync();
        } finally {
            group.shutdownGracefully();
            worker.interrupt();
            announcer.interrupt();
            controlWorker.interrupt();
        }
    }

    static class UdpPacketHandler extends SimpleChannelInboundHandler<DatagramPacket> {
        private final ConcurrentLinkedQueue<byte[]> queue;

        public UdpPacketHandler(ConcurrentLinkedQueue<byte[]> queue) {
            this.queue = queue;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket packet) {
            ByteBuf content = packet.content();
            byte[] bytes = new byte[content.readableBytes()];
            content.readBytes(bytes);
            queue.add(bytes);
        }
    }

    static class MulticastAnnouncer extends Thread {
        private final ConcurrentLinkedQueue<byte[]> telemetryQueue;
        private final String aggregatorId;
        private final String multicastGroup;
        private final int multicastPort;
        private final String ipAddress;

        public MulticastAnnouncer(ConcurrentLinkedQueue<byte[]> telemetryQueue) {
            this.telemetryQueue = telemetryQueue;
            this.aggregatorId = System.getenv().getOrDefault("AGGREGATOR_ID", "java_netty_1");
            this.multicastGroup = System.getenv().getOrDefault("MULTICAST_GROUP", "239.0.0.1");
            this.multicastPort = Integer.parseInt(System.getenv().getOrDefault("MULTICAST_PORT", "5005"));
            this.ipAddress = getLocalIp();
        }

        private String getLocalIp() {
            try {
                return InetAddress.getLocalHost().getHostAddress();
            } catch (Exception e) {
                return "127.0.0.1";
            }
        }

        @Override
        public void run() {
            System.out.println("Multicast Announcer iniciado no grupo " + multicastGroup + ":" + multicastPort);
            try (MulticastSocket socket = new MulticastSocket()) {
                InetAddress group = InetAddress.getByName(multicastGroup);
                while (!Thread.currentThread().isInterrupted()) {
                    AggregatorLoad loadMsg = AggregatorLoad.newBuilder()
                            .setAggregatorId(aggregatorId)
                            .setIpAddress(ipAddress)
                            .setTelemetryPort(5000)
                            .setDiscoveryPort(5002)
                            // Base fixa equivalente à do agregador Rust (5.0). Antes era
                            // availableProcessors()*5.0 (~40), o que inflava o score e fazia
                            // os sensores SEMPRE escolherem o Rust — o Java ficava ocioso.
                            // Com bases iguais, o tamanho da fila passa a decidir a rota.
                            .setCpuLoad(5.0)
                            .setQueueSize(telemetryQueue.size())
                            .setTimestamp(System.currentTimeMillis() / 1000)
                            .build();

                    byte[] payload = loadMsg.toByteArray();
                    java.net.DatagramPacket packet = new java.net.DatagramPacket(payload, payload.length, group, multicastPort);
                    socket.send(packet);
                    System.out.println("Anunciado Load: " + loadMsg.getQueueSize() + " itens na fila.");
                    Thread.sleep(2000);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                System.err.println("Erro no MulticastAnnouncer: " + e.getMessage());
            }
        }
    }

    static class RedisWriterWorker extends Thread {
        private final ConcurrentLinkedQueue<byte[]> telemetryQueue;
        private final ConcurrentLinkedQueue<byte[]> discoveryQueue;
        private final JedisPool jedisPool;

        public RedisWriterWorker(ConcurrentLinkedQueue<byte[]> telemetryQueue, ConcurrentLinkedQueue<byte[]> discoveryQueue) {
            this.telemetryQueue = telemetryQueue;
            this.discoveryQueue = discoveryQueue;

            String redisHost = System.getenv().getOrDefault("REDIS_HOST", "localhost");
            int redisPort = Integer.parseInt(System.getenv().getOrDefault("REDIS_PORT", "6379"));
            
            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setMaxTotal(8);
            this.jedisPool = new JedisPool(poolConfig, redisHost, redisPort);
        }

        private static final byte[] AES_KEY;
        static {
            String rawKey = readSecret("AES_SECRET_KEY", "SmartCityKey1234");
            byte[] keyBytes = new byte[16];
            byte[] rawBytes = rawKey.getBytes();
            System.arraycopy(rawBytes, 0, keyBytes, 0, Math.min(rawBytes.length, 16));
            AES_KEY = keyBytes;
        }
        private static final SecureRandom secureRandom = new SecureRandom();

        // Trimming aproximado dos streams Redis: limita o crescimento ilimitado
        // (antes os xadd não tinham MAXLEN e a memória do Redis crescia sem fim).
        private static final long MAX_STREAM_LEN = 100_000L;
        private static final XAddParams STREAM_TRIM =
            XAddParams.xAddParams().maxLen(MAX_STREAM_LEN).approximateTrimming();

        private byte[] encryptPayload(byte[] payload) throws Exception {
            byte[] nonce = new byte[12];
            secureRandom.nextBytes(nonce);
            
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            GCMParameterSpec spec = new GCMParameterSpec(128, nonce);
            SecretKeySpec keySpec = new SecretKeySpec(AES_KEY, "AES");
            cipher.init(Cipher.ENCRYPT_MODE, keySpec, spec);
            
            byte[] cipherText = cipher.doFinal(payload);
            
            ByteBuffer byteBuffer = ByteBuffer.allocate(nonce.length + cipherText.length);
            byteBuffer.put(nonce);
            byteBuffer.put(cipherText);
            return byteBuffer.array();
        }

        @Override
        public void run() {
            System.out.println("Redis Writer Worker iniciado.");
            String aggIdStr = System.getenv().getOrDefault("AGGREGATOR_ID", "java_netty_1");
            byte[] aggregatorId = aggIdStr.getBytes();
            long lastHeartbeat = 0L;
            while (!Thread.currentThread().isInterrupted()) {
                boolean didWork = false;
                try (Jedis jedis = jedisPool.getResource()) {
                    while (!telemetryQueue.isEmpty()) {
                        byte[] payload = telemetryQueue.poll();
                        if (payload != null) {
                            byte[] encrypted = encryptPayload(payload);
                            Map<byte[], byte[]> map = new HashMap<>();
                            map.put("payload".getBytes(), encrypted);
                            map.put("aggregator".getBytes(), aggregatorId);
                            jedis.xadd("telemetry_stream".getBytes(), STREAM_TRIM, map);
                            didWork = true;
                        }
                    }
                    while (!discoveryQueue.isEmpty()) {
                        byte[] payload = discoveryQueue.poll();
                        if (payload != null) {
                            byte[] encrypted = encryptPayload(payload);
                            Map<byte[], byte[]> map = new HashMap<>();
                            map.put("payload".getBytes(), encrypted);
                            map.put("aggregator".getBytes(), aggregatorId);
                            jedis.xadd("discovery_stream".getBytes(), STREAM_TRIM, map);
                            didWork = true;
                        }
                    }

                    // [Fase E] Heartbeat de saúde (TTL 30s), lido pelo gateway.
                    long nowMs = System.currentTimeMillis();
                    if (nowMs - lastHeartbeat >= 2000L) {
                        jedis.setex("agg_heartbeat:" + aggIdStr, 30L, String.valueOf(nowMs / 1000L));
                        lastHeartbeat = nowMs;
                    }
                } catch (Exception e) {
                    System.err.println("Erro ao conectar ou gravar no Redis: " + e.getMessage());
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }

                if (!didWork) {
                    try {
                        Thread.sleep(5);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
            jedisPool.close();
            System.out.println("Redis Writer Worker finalizado.");
        }
    }

    static class ControlCommandWorker extends Thread {
        private final EventLoopGroup group;
        private final ConcurrentLinkedQueue<byte[]> telemetryQueue;
        private final JedisPool jedisPool;
        private final String aggregatorId;

        public ControlCommandWorker(EventLoopGroup group, ConcurrentLinkedQueue<byte[]> telemetryQueue) {
            this.group = group;
            this.telemetryQueue = telemetryQueue;
            this.aggregatorId = System.getenv().getOrDefault("AGGREGATOR_ID", "java_netty_1");
            
            String redisHost = System.getenv().getOrDefault("REDIS_HOST", "localhost");
            int redisPort = Integer.parseInt(System.getenv().getOrDefault("REDIS_PORT", "6379"));
            
            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setMaxTotal(2);
            this.jedisPool = new JedisPool(poolConfig, redisHost, redisPort);
        }

        @Override
        public void run() {
            System.out.println("Control Command Worker iniciado.");
            String channel = "agg_control_" + aggregatorId;
            while (!Thread.currentThread().isInterrupted()) {
                try (Jedis jedis = jedisPool.getResource()) {
                    java.util.List<String> res = jedis.blpop(0, channel);
                    if (res != null && res.size() == 2) {
                        String payload = res.get(1);
                        if (payload.startsWith("ABRIR_PORTA_UDP: ")) {
                            try {
                                int port = Integer.parseInt(payload.substring(17).trim());
                                if (!activePorts.containsKey(port)) {
                                    System.out.println("Comando recebido: abrindo nova porta UDP " + port);
                                    Bootstrap b = new Bootstrap();
                                    b.group(group)
                                     .channel(NioDatagramChannel.class)
                                     .option(ChannelOption.SO_RCVBUF, 1024 * 1024)
                                     .handler(new ChannelInitializer<NioDatagramChannel>() {
                                         @Override
                                         protected void initChannel(NioDatagramChannel ch) {
                                             ch.pipeline().addLast(new UdpPacketHandler(telemetryQueue));
                                         }
                                     });
                                    ChannelFuture future = b.bind("0.0.0.0", port).sync();
                                    activePorts.put(port, future.channel());
                                }
                            } catch (Exception e) {
                                System.err.println("Erro ao abrir nova porta: " + e.getMessage());
                            }
                        } else if (payload.startsWith("FECHAR_PORTA_UDP: ")) {
                            try {
                                int port = Integer.parseInt(payload.substring(18).trim());
                                io.netty.channel.Channel nettyChannel = activePorts.remove(port);
                                if (nettyChannel != null) {
                                    System.out.println("Comando recebido: fechando porta UDP " + port);
                                    nettyChannel.close().sync();
                                }
                            } catch (Exception e) {
                                System.err.println("Erro ao fechar porta: " + e.getMessage());
                            }
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Erro no Control Command Worker: " + e.getMessage());
                    try { Thread.sleep(1000); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                }
            }
            jedisPool.close();
        }
    }
}
