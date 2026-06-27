import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * Criptografia + anti-replay do canal de controle (semáforo Java).
 *
 * Esquema idêntico ao do gateway/Python e da câmera:
 *   Frame cifrado = nonce[12] || AES-128-GCM(ciphertext + tag[16])
 *   Chave         = AES_SECRET_KEY (UTF-8, padding/truncate p/ 16 bytes)
 *   Anti-replay   = timestamp (janela de skew) + cache de command_id.
 *
 * Ligado por CONTROL_SECURE=1. Compartilhado por sensor.java e Console.java.
 * (A interop AES-GCM Java↔Python já é exercida pelo agregador Java → gateway.)
 */
public final class ControlCrypto {

    public static final boolean SECURE = parseBool(System.getenv("CONTROL_SECURE"));
    public static final int MAX_SKEW_SECS = parseInt(System.getenv("CONTROL_MAX_SKEW_SECS"), 30);

    private static final byte[] KEY = deriveKey(readSecret("AES_SECRET_KEY", "SmartCityKey1234"));
    private static final SecureRandom RNG = new SecureRandom();
    private static final int NONCE_LEN = 12;
    private static final int TAG_BITS = 128;

    private ControlCrypto() {}

    private static boolean parseBool(String v) {
        if (v == null) return false;
        v = v.trim().toLowerCase();
        return v.equals("1") || v.equals("true") || v.equals("yes") || v.equals("on");
    }

    private static int parseInt(String v, int def) {
        try { return v == null ? def : Integer.parseInt(v.trim()); }
        catch (NumberFormatException e) { return def; }
    }

    private static String readSecret(String name, String def) {
        String filePath = System.getenv(name + "_FILE");
        if (filePath != null && !filePath.isEmpty()) {
            try {
                return new String(Files.readAllBytes(Paths.get(filePath)), StandardCharsets.UTF_8).trim();
            } catch (Exception e) {
                System.err.println("Falha ao ler " + name + "_FILE: " + e.getMessage());
            }
        }
        String v = System.getenv(name);
        return v != null ? v : def;
    }

    static byte[] deriveKey(String raw) {
        byte[] key = new byte[16];
        byte[] rawBytes = raw.getBytes(StandardCharsets.UTF_8);
        System.arraycopy(rawBytes, 0, key, 0, Math.min(rawBytes.length, 16));
        return key;
    }

    static byte[] wrapWith(byte[] key, byte[] plaintext) throws Exception {
        byte[] nonce = new byte[NONCE_LEN];
        RNG.nextBytes(nonce);
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(key, "AES"), new GCMParameterSpec(TAG_BITS, nonce));
        byte[] ct = cipher.doFinal(plaintext);
        byte[] out = new byte[NONCE_LEN + ct.length];
        System.arraycopy(nonce, 0, out, 0, NONCE_LEN);
        System.arraycopy(ct, 0, out, NONCE_LEN, ct.length);
        return out;
    }

    static byte[] unwrapWith(byte[] key, byte[] blob) throws Exception {
        if (blob.length < NONCE_LEN + 16) {
            throw new IllegalArgumentException("frame cifrado muito curto");
        }
        byte[] nonce = new byte[NONCE_LEN];
        System.arraycopy(blob, 0, nonce, 0, NONCE_LEN);
        byte[] ct = new byte[blob.length - NONCE_LEN];
        System.arraycopy(blob, NONCE_LEN, ct, 0, ct.length);
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(key, "AES"), new GCMParameterSpec(TAG_BITS, nonce));
        return cipher.doFinal(ct);
    }

    public static byte[] wrap(byte[] plaintext) throws Exception {
        return wrapWith(KEY, plaintext);
    }

    public static byte[] unwrap(byte[] blob) throws Exception {
        return unwrapWith(KEY, blob);
    }

    /** Rejeita comandos fora da janela de tempo ou com command_id repetido. */
    public static final class ReplayGuard {
        private final int maxSkew;
        private final int capacity;
        private final LinkedHashMap<String, Long> seen;

        public ReplayGuard() { this(MAX_SKEW_SECS, 512); }

        public ReplayGuard(int maxSkew, int capacity) {
            this.maxSkew = maxSkew;
            this.capacity = capacity;
            this.seen = new LinkedHashMap<String, Long>(16, 0.75f, false) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Long> eldest) {
                    return size() > ReplayGuard.this.capacity;
                }
            };
        }

        /** Retorna null se o comando é aceito, ou uma string de motivo se rejeitado. */
        public synchronized String check(String commandId, long ts) {
            long now = System.currentTimeMillis() / 1000L;
            if (ts != 0 && Math.abs(now - ts) > maxSkew) {
                return "timestamp fora da janela (±" + maxSkew + "s)";
            }
            if (commandId != null && !commandId.isEmpty()) {
                if (seen.containsKey(commandId)) {
                    return "command_id repetido (replay)";
                }
                seen.put(commandId, now);
            }
            return null;
        }
    }
}
