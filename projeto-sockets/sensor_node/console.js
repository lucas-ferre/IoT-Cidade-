// ====================================================================
// Console de controle do Sensor de Enchente (Node.js)
//
// Modos:
//   STANDALONE:  docker exec -it sensor_enchente node console.js [host] [porta]
//   EMBUTIDO:    SENSOR_IDLE_CONSOLE=1 + docker attach sensor_enchente
//
// Fala o protocolo length-prefix (>I4) + Protobuf ConfigCommand, com cripto
// AES-GCM se CONTROL_SECURE=1. Uma conexão por comando.
// Comandos: status, on, off, err, freq <s>, help, quit.
// ====================================================================

const net = require('net');
const readline = require('readline');
const protobuf = require('protobufjs');
const cc = require('./control_crypto');

const DEFAULT_HOST = '127.0.0.1';
const DEFAULT_PORT = 5010;
const MAX_FRAME = 1024 * 1024;

let ConfigCommand, ConfigResponse, DeviceStatus;

const HELP = `
Comandos do console (sensor de enchente):
  status [device_id]            Lê o estado atual.
  on     [device_id]            Liga (STATUS_ON).
  off    [device_id]            Desliga (STATUS_OFF).
  err    [device_id]            Marca falha (STATUS_ERROR).
  freq <segundos> [device_id]   Altera o intervalo de telemetria.
  help                          Mostra esta ajuda.
  quit / exit                   Sai do console.`;

async function loadProto() {
  const root = await protobuf.load('messages.proto');
  ConfigCommand = root.lookupType('smartcity.ConfigCommand');
  ConfigResponse = root.lookupType('smartcity.ConfigResponse');
  DeviceStatus = root.lookupEnum('smartcity.DeviceStatus').values;
}

function toNum(v) {
  if (v == null) return 0;
  if (typeof v === 'object' && typeof v.toNumber === 'function') return v.toNumber();
  return Number(v);
}

function sendCommand(host, port, fields) {
  return new Promise((resolve) => {
    const cmd = ConfigCommand.create({
      commandId: `CONSOLE-${Math.floor(Math.random() * 1e6)}`,
      timestamp: Math.floor(Date.now() / 1000),
      updateStatus: fields.updateStatus || false,
      targetStatus: fields.targetStatus || DeviceStatus.STATUS_ON,
      updateFrequency: fields.updateFrequency || false,
      newFrequencySecs: fields.newFrequencySecs || 0,
      targetDeviceId: fields.targetDeviceId || '',
    });
    let body = Buffer.from(ConfigCommand.encode(cmd).finish());
    if (cc.SECURE) body = cc.wrap(body);
    const header = Buffer.alloc(4); header.writeUInt32BE(body.length, 0);

    const sock = net.createConnection({ host, port });
    let buf = Buffer.alloc(0);
    let expected = null;
    sock.setTimeout(5000);
    sock.on('connect', () => sock.write(Buffer.concat([header, body])));
    sock.on('data', (chunk) => {
      buf = Buffer.concat([buf, chunk]);
      if (expected === null && buf.length >= 4) {
        expected = buf.readUInt32BE(0);
        if (expected <= 0 || expected > MAX_FRAME) { sock.destroy(); resolve({ error: `frame inválido: ${expected}` }); return; }
        buf = buf.subarray(4);
      }
      if (expected !== null && buf.length >= expected) {
        let respBuf = buf.subarray(0, expected);
        sock.destroy();
        try {
          if (cc.SECURE) respBuf = cc.unwrap(respBuf);
          resolve({ resp: ConfigResponse.decode(respBuf) });
        } catch (e) { resolve({ error: e.message }); }
      }
    });
    sock.on('timeout', () => { sock.destroy(); resolve({ error: 'timeout' }); });
    sock.on('error', (e) => resolve({ error: e.message }));
  });
}

function statusName(v) {
  for (const [k, val] of Object.entries(DeviceStatus)) if (val === v) return k;
  return String(v);
}

async function dispatch(host, port, line) {
  const parts = line.trim().split(/\s+/).filter(Boolean);
  if (!parts.length) return true;
  const cmd = parts[0].toLowerCase();
  if (cmd === 'quit' || cmd === 'exit') return false;
  if (cmd === 'help' || cmd === '?') { console.log(HELP); return true; }

  let fields = null, devIdx = 1;
  if (cmd === 'status') { fields = {}; devIdx = 1; }
  else if (cmd === 'on') { fields = { updateStatus: true, targetStatus: DeviceStatus.STATUS_ON }; }
  else if (cmd === 'off') { fields = { updateStatus: true, targetStatus: DeviceStatus.STATUS_OFF }; }
  else if (cmd === 'err') { fields = { updateStatus: true, targetStatus: DeviceStatus.STATUS_ERROR }; }
  else if (cmd === 'freq') {
    const secs = parseInt(parts[1], 10);
    if (!secs || secs <= 0) { console.log('  Uso: freq <segundos> [device_id]'); return true; }
    fields = { updateFrequency: true, newFrequencySecs: secs }; devIdx = 2;
  } else { console.log(`  Comando desconhecido: '${cmd}'. Digite 'help'.`); return true; }

  fields.targetDeviceId = parts[devIdx] || '';
  const { resp, error } = await sendCommand(host, port, fields);
  if (error) console.log(`  ✗ Falha de comunicação com ${host}:${port}: ${error}`);
  else console.log(`  ${resp.success ? '✓' : '✗'} ${resp.message || '(sem mensagem)'}\n    status=${statusName(resp.updatedStatus)} | frequência=${toNum(resp.updatedFrequencySecs)}s | cmd=${resp.commandId}`);
  return true;
}

async function runConsole(host, port) {
  await loadProto();
  console.log('============================================================');
  console.log(`[Console Node] Controle do sensor de enchente (${host}:${port}).`);
  console.log("[Console Node] Digite 'help' para os comandos, 'quit' para sair.");
  console.log('============================================================');
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout, prompt: 'enchente> ' });
  rl.prompt();
  rl.on('line', async (line) => {
    const keep = await dispatch(host, port, line);
    if (!keep) { rl.close(); return; }
    rl.prompt();
  });
  rl.on('close', () => { console.log('[Console Node] Console encerrado.'); });
}

function startEmbedded(port) {
  // Já estamos no processo do sensor; só inicia o REPL ligado ao próprio controle.
  runConsole('127.0.0.1', port || DEFAULT_PORT).catch((e) => console.error(`[Console Node] erro: ${e.message}`));
}

if (require.main === module) {
  const host = process.argv[2] || DEFAULT_HOST;
  const port = parseInt(process.argv[3], 10) || DEFAULT_PORT;
  runConsole(host, port).catch((e) => { console.error(`[Console Node] erro: ${e.message}`); process.exit(1); });
}

module.exports = { runConsole, startEmbedded };
