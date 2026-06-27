# ====================================================================
# Criptografia + anti-replay do canal de controle (sensor de ruído, Ruby)
#
# Esquema idêntico ao gateway/Python, semáforo/Java, poste/Lua e enchente/Node:
#   Frame cifrado = nonce[12] || AES-128-GCM(ciphertext + tag[16])
#   Chave         = AES_SECRET_KEY (UTF-8, padding/truncate p/ 16 bytes)
#   Anti-replay   = timestamp (janela de skew) + cache de command_id.
#
# Ligado por CONTROL_SECURE=1. Compartilhado por sensor.rb e console.rb.
# ====================================================================

require 'openssl'
require 'thread'

module ControlCrypto
  def self.get_secret(name, default)
    file_path = ENV["#{name}_FILE"]
    if file_path && File.exist?(file_path)
      begin
        return File.read(file_path).strip
      rescue StandardError
        # cai para a env var
      end
    end
    ENV[name] || default
  end

  raw_key = get_secret('AES_SECRET_KEY', 'SmartCityKey1234')
  KEY = raw_key.b.ljust(16, "\x00".b)[0, 16]
  SECURE = %w[1 true yes on].include?((ENV['CONTROL_SECURE'] || '').downcase)
  MAX_SKEW_SECS = [(ENV['CONTROL_MAX_SKEW_SECS'] || '30').to_i, 1].max
  NONCE_LEN = 12
  TAG_LEN = 16

  def self.wrap(plaintext)
    cipher = OpenSSL::Cipher.new('aes-128-gcm')
    cipher.encrypt
    cipher.key = KEY
    iv = OpenSSL::Random.random_bytes(NONCE_LEN)
    cipher.iv = iv
    ciphertext = cipher.update(plaintext) + cipher.final
    iv + ciphertext + cipher.auth_tag(TAG_LEN)
  end

  def self.unwrap(blob)
    raise 'frame cifrado muito curto' if blob.bytesize < NONCE_LEN + TAG_LEN

    iv  = blob.byteslice(0, NONCE_LEN)
    tag = blob.byteslice(blob.bytesize - TAG_LEN, TAG_LEN)
    ct  = blob.byteslice(NONCE_LEN, blob.bytesize - NONCE_LEN - TAG_LEN)

    cipher = OpenSSL::Cipher.new('aes-128-gcm')
    cipher.decrypt
    cipher.key = KEY
    cipher.iv = iv
    cipher.auth_tag = tag
    cipher.update(ct) + cipher.final
  end

  # Rejeita comandos fora da janela de tempo ou com command_id repetido.
  class ReplayGuard
    def initialize(max_skew = MAX_SKEW_SECS, capacity = 512)
      @max_skew = max_skew
      @capacity = capacity
      @seen = {}
      @mutex = Mutex.new
    end

    # Retorna [ok(bool), motivo(String|nil)].
    def check(command_id, ts)
      now = Time.now.to_i
      if ts && ts != 0 && (now - ts).abs > @max_skew
        return [false, "timestamp fora da janela (±#{@max_skew}s)"]
      end
      @mutex.synchronize do
        if command_id && !command_id.empty?
          return [false, 'command_id repetido (replay)'] if @seen.key?(command_id)

          @seen[command_id] = now
          @seen.shift while @seen.size > @capacity
        end
      end
      [true, nil]
    end
  end
end
