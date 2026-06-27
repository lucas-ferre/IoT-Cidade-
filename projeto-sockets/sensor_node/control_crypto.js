// ====================================================================
// Criptografia + anti-replay do canal de controle (sensor de enchente, Node.js)
//
// Esquema idêntico ao do gateway/Python, semáforo/Java e poste/Lua:
//   Frame cifrado = nonce[12] || AES-128-GCM(ciphertext + tag[16])
//   Chave         = AES_SECRET_KEY (UTF-8, padding/truncate p/ 16 bytes)
//   Anti-replay   = timestamp (janela de skew) + cache de command_id.
//
// Ligado por CONTROL_SECURE=1. Compartilhado por sensor.js e console.js.
// ====================================================================

const crypto = require('crypto');
const fs = require('fs');

function getSecret(name, def) {
  const filePath = process.env[name + '_FILE'];
  if (filePath) {
    try {
      return fs.readFileSync(filePath, 'utf8').trim();
    } catch (e) {
      // cai para a env var
    }
  }
  return process.env[name] || def;
}

const rawKey = getSecret('AES_SECRET_KEY', 'SmartCityKey1234');
const KEY = Buffer.alloc(16);
Buffer.from(rawKey, 'utf8').copy(KEY, 0, 0, Math.min(16, Buffer.byteLength(rawKey, 'utf8')));

const SECURE = ['1', 'true', 'yes', 'on'].includes(String(process.env.CONTROL_SECURE || '').toLowerCase());
const MAX_SKEW_SECS = Math.max(1, parseInt(process.env.CONTROL_MAX_SKEW_SECS || '30', 10) || 30);

const NONCE_LEN = 12;
const TAG_LEN = 16;

function wrap(plaintext) {
  const iv = crypto.randomBytes(NONCE_LEN);
  const cipher = crypto.createCipheriv('aes-128-gcm', KEY, iv);
  const ct = Buffer.concat([cipher.update(plaintext), cipher.final()]);
  const tag = cipher.getAuthTag();
  return Buffer.concat([iv, ct, tag]);
}

function unwrap(blob) {
  if (blob.length < NONCE_LEN + TAG_LEN) {
    throw new Error('frame cifrado muito curto');
  }
  const iv = blob.subarray(0, NONCE_LEN);
  const tag = blob.subarray(blob.length - TAG_LEN);
  const ct = blob.subarray(NONCE_LEN, blob.length - TAG_LEN);
  const decipher = crypto.createDecipheriv('aes-128-gcm', KEY, iv);
  decipher.setAuthTag(tag);
  return Buffer.concat([decipher.update(ct), decipher.final()]);
}

class ReplayGuard {
  constructor(maxSkew = MAX_SKEW_SECS, capacity = 512) {
    this.maxSkew = maxSkew;
    this.capacity = capacity;
    this.seen = new Map();
  }

  // Retorna [ok, motivoOuNull].
  check(commandId, ts) {
    const now = Math.floor(Date.now() / 1000);
    if (ts && Math.abs(now - ts) > this.maxSkew) {
      return [false, `timestamp fora da janela (±${this.maxSkew}s)`];
    }
    if (commandId) {
      if (this.seen.has(commandId)) {
        return [false, 'command_id repetido (replay)'];
      }
      this.seen.set(commandId, now);
      while (this.seen.size > this.capacity) {
        this.seen.delete(this.seen.keys().next().value);
      }
    }
    return [true, null];
  }
}

module.exports = { SECURE, MAX_SKEW_SECS, getSecret, wrap, unwrap, ReplayGuard };
