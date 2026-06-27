import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Instant;
import java.util.UUID;

import smartcity.Messages;

/**
 * Console de controle interativo do Semáforo (Java).
 *
 * Modos de uso:
 *
 * 1. STANDALONE (processo separado) — conecta na porta de controle TCP do sensor
 *    e envia ConfigCommand (mesmo contrato do Gateway/Dashboard):
 *
 *        docker exec -it sensor_semaforo java -cp .:protobuf.jar Console
 *        # ou: java -cp .:protobuf.jar Console <host> <porta>
 *
 * 2. EMBUTIDO (IDLE no próprio processo do sensor) — sensor.java inicia
 *    runConsole() em uma thread quando SENSOR_IDLE_CONSOLE está ligado.
 *    Requer terminal anexado (docker compose: stdin_open + tty):
 *
 *        docker attach sensor_semaforo      # Ctrl-P Ctrl-Q para desanexar
 *
 * Protocolo: prefixo de 4 bytes (big-endian) + Protobuf ConfigCommand, uma
 * conexão por comando. Comandos: status, on, off, err, freq, help, quit.
 */
public class Console {

    private static final String DEFAULT_HOST = "127.0.0.1";
    private static final int DEFAULT_PORT = 5003; // CONTROL_TCP_PORT do semáforo
    private static final int CONNECT_TIMEOUT_MS = 5000;
    private static final int MAX_FRAME_BYTES = 1024 * 1024;

    private static final String HELP_TEXT =
        "\nComandos do console (semáforo Java):\n" +
        "  status [device_id]            Lê o estado atual (não altera nada).\n" +
        "  on     [device_id]            Liga o dispositivo (STATUS_ON).\n" +
        "  off    [device_id]            Desliga o dispositivo (STATUS_OFF).\n" +
        "  err    [device_id]            Marca falha (STATUS_ERROR).\n" +
        "  freq <segundos> [device_id]   Altera o intervalo de telemetria.\n" +
        "  help                          Mostra esta ajuda.\n" +
        "  quit / exit                   Sai do console.\n" +
        "\nSem device_id, o sensor aplica ao dispositivo padrão (primeiro da frota).";

    public static void main(String[] args) {
        String host = args.length > 0 ? args[0] : DEFAULT_HOST;
        int port = args.length > 1 ? Integer.parseInt(args[1]) : DEFAULT_PORT;
        runConsole(host, port);
    }

    /** Loop interativo (IDLE). Usado no modo standalone e no embutido. */
    public static void runConsole(String host, int port) {
        System.out.println("============================================================");
        System.out.println("[Console Java] Interface de controle do semáforo (" + host + ":" + port + ").");
        System.out.println("[Console Java] Digite 'help' para ver os comandos, 'quit' para sair.");
        System.out.println("============================================================");

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        try {
            while (true) {
                System.out.print("semaforo> ");
                System.out.flush();
                String line = br.readLine();
                if (line == null) {
                    System.out.println("\n[Console Java] EOF — encerrando console.");
                    return;
                }
                if (!dispatch(host, port, line.trim())) {
                    System.out.println("[Console Java] Console encerrado.");
                    return;
                }
            }
        } catch (Exception e) {
            System.err.println("[Console Java] Erro fatal no console: " + e.getMessage());
        }
    }

    private static boolean dispatch(String host, int port, String line) {
        if (line.isEmpty()) {
            return true;
        }
        String[] parts = line.split("\\s+");
        String cmd = parts[0].toLowerCase();

        try {
            switch (cmd) {
                case "quit":
                case "exit":
                    return false;
                case "help":
                case "?":
                    System.out.println(HELP_TEXT);
                    return true;
                case "status": {
                    String dev = parts.length > 1 ? parts[1] : "";
                    printResponse(sendCommand(host, port, false, Messages.DeviceStatus.STATUS_ON,
                                              false, 0, dev));
                    return true;
                }
                case "on":
                case "off":
                case "err": {
                    Messages.DeviceStatus target =
                        cmd.equals("on") ? Messages.DeviceStatus.STATUS_ON :
                        cmd.equals("off") ? Messages.DeviceStatus.STATUS_OFF :
                        Messages.DeviceStatus.STATUS_ERROR;
                    String dev = parts.length > 1 ? parts[1] : "";
                    printResponse(sendCommand(host, port, true, target, false, 0, dev));
                    return true;
                }
                case "freq": {
                    if (parts.length < 2 || !parts[1].matches("\\d+") || Integer.parseInt(parts[1]) <= 0) {
                        System.out.println("  Uso: freq <segundos> [device_id]  (segundos > 0)");
                        return true;
                    }
                    int secs = Integer.parseInt(parts[1]);
                    String dev = parts.length > 2 ? parts[2] : "";
                    printResponse(sendCommand(host, port, false, Messages.DeviceStatus.STATUS_ON,
                                              true, secs, dev));
                    return true;
                }
                default:
                    System.out.println("  Comando desconhecido: '" + cmd + "'. Digite 'help'.");
                    return true;
            }
        } catch (Exception e) {
            System.out.println("  ✗ Falha de comunicação com " + host + ":" + port + ": " + e.getMessage());
            return true;
        }
    }

    private static Messages.ConfigResponse sendCommand(
            String host, int port,
            boolean updateStatus, Messages.DeviceStatus targetStatus,
            boolean updateFrequency, int newFrequencySecs,
            String targetDeviceId) throws Exception {

        Messages.ConfigCommand cmd = Messages.ConfigCommand.newBuilder()
            .setCommandId("CONSOLE-" + UUID.randomUUID().toString().substring(0, 6).toUpperCase())
            .setTimestamp(Instant.now().getEpochSecond())
            .setUpdateStatus(updateStatus)
            .setTargetStatus(targetStatus)
            .setUpdateFrequency(updateFrequency)
            .setNewFrequencySecs(newFrequencySecs)
            .setTargetDeviceId(targetDeviceId)
            .build();

        byte[] payload = cmd.toByteArray();
        if (ControlCrypto.SECURE) {
            payload = ControlCrypto.wrap(payload);
        }

        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(host, port), CONNECT_TIMEOUT_MS);
            s.setSoTimeout(CONNECT_TIMEOUT_MS);
            DataOutputStream out = new DataOutputStream(s.getOutputStream());
            DataInputStream in = new DataInputStream(s.getInputStream());

            out.writeInt(payload.length);
            out.write(payload);
            out.flush();

            int len = in.readInt();
            if (len <= 0 || len > MAX_FRAME_BYTES) {
                throw new Exception("frame de resposta inválido: " + len + " bytes");
            }
            byte[] respBuf = new byte[len];
            in.readFully(respBuf);
            if (ControlCrypto.SECURE) {
                respBuf = ControlCrypto.unwrap(respBuf);
            }
            return Messages.ConfigResponse.parseFrom(respBuf);
        }
    }

    private static void printResponse(Messages.ConfigResponse resp) {
        String ok = resp.getSuccess() ? "✓" : "✗";
        String msg = resp.getMessage().isEmpty() ? "(sem mensagem)" : resp.getMessage();
        System.out.println("  " + ok + " " + msg);
        System.out.println("    status=" + resp.getUpdatedStatus()
            + " | frequência=" + resp.getUpdatedFrequencySecs() + "s"
            + " | cmd=" + resp.getCommandId());
    }
}
