// ====================================================================
// Sensor de Enchente / Nível d'Água (Node.js) — CONTROLÁVEL
//
// Segue o mesmo ciclo de vida dos demais sensores: load-balancer via multicast,
// descoberta (UDP), autenticação (TCP), telemetria (UDP), servidor de controle
// (TCP, com cripto AES-GCM + anti-replay da Fase A.2), heartbeat e shutdown
// gracioso. Métricas: water_level (cm) e flow_rate (L/s).
// ====================================================================

const dgram = require('dgram');
const net = require('net');
const os = require('os');
const protobuf = require('protobufjs');
const cc = require('./control_crypto');

const GATEWAY_HOST = process.env.GATEWAY_HOST || 'gateway';
const GATEWAY_DISCOVERY_PORT = 5002;
const AUTH_TCP_PORT = 5007;
const CONTROL_TCP_PORT = 5010;
const MULTICAST_GROUP = '239.0.0.1';
const MULTICAST_PORT = 5005;
const SENSOR_LICENSE_PART = process.env.SENSOR_LICENSE_PART || 'V1-FULL';
const SENSOR_HEX_CODE = '0E';
const MAX_TCP_FRAME_BYTES = 1024 * 1024;
const MANUAL_OVERRIDE_SECS = 30.0;
const THRESHOLD_EVENT_COOLDOWN_SECS = 3.0;
const HEARTBEAT_INTERVAL_SECS = Math.max(1, parseFloat(process.env.SENSOR_HEARTBEAT_INTERVAL_SECS || '10') || 10);
const HEARTBEAT_JITTER_SECS = Math.max(0, parseFloat(process.env.SENSOR_HEARTBEAT_JITTER_SECS || '2') || 2);
const WATER_LEVEL_THRESHOLD = parseFloat(process.env.WATER_LEVEL_THRESHOLD || '150');
const FLOOD_DEVICE_COUNT = Math.max(1, parseInt(process.env.FLOOD_DEVICE_COUNT || '4', 10) || 4);

const SECTORS = [
  ['Pici', 'pici'], ['Benfica', 'benfica'],
  ['Porangabussu', 'porangabussu'], ['Labomar', 'labomar'],
];

let GATEWAY_TELEMETRY_PORT = 5000;
let bestAggregatorIp = GATEWAY_HOST;
let bestAggregatorScore = 999999.0;
let shuttingDown = false;
let DEVICE_IP = '127.0.0.1';

const devices = new Map();
const replayGuard = new cc.ReplayGuard();

// Tipos protobuf (preenchidos em loadProto)
let DataPayload, DiscoveryResponse, AuthRequest, AuthResponse,
    AggregatorLoad, ConfigCommand, ConfigResponse, DeviceType, DeviceStatus;

let txSock; // socket UDP de envio reutilizado

function randInt(min, max) { return Math.floor(Math.random() * (max - min + 1)) + min; }
function nowSecs() { return Math.floor(Date.now() / 1000); }
function monoSecs() { return process.hrtime()[0] + process.hrtime()[1] / 1e9; }

function toNum(v) {
  if (v == null) return 0;
  if (typeof v === 'object' && typeof v.toNumber === 'function') return v.toNumber();
  return Number(v);
}

async function loadProto() {
  const root = await protobuf.load('messages.proto');
  DataPayload = root.lookupType('smartcity.DataPayload');
  DiscoveryResponse = root.lookupType('smartcity.DiscoveryResponse');
  AuthRequest = root.lookupType('smartcity.AuthRequest');
  AuthResponse = root.lookupType('smartcity.AuthResponse');
  AggregatorLoad = root.lookupType('smartcity.AggregatorLoad');
  ConfigCommand = root.lookupType('smartcity.ConfigCommand');
  ConfigResponse = root.lookupType('smartcity.ConfigResponse');
  DeviceType = root.lookupEnum('smartcity.DeviceType').values;
  DeviceStatus = root.lookupEnum('smartcity.DeviceStatus').values;
}

function getLocalIp() {
  return new Promise((resolve) => {
    const s = dgram.createSocket('udp4');
    s.on('error', () => { try { s.close(); } catch (e) {} resolve('127.0.0.1'); });
    try {
      s.connect(5000, GATEWAY_HOST, () => {
        try {
          const addr = s.address().address;
          s.close();
          resolve(addr || '127.0.0.1');
        } catch (e) { try { s.close(); } catch (_) {} resolve('127.0.0.1'); }
      });
    } catch (e) { resolve('127.0.0.1'); }
  });
}

function statusName(v) {
  for (const [k, val] of Object.entries(DeviceStatus)) if (val === v) return k;
  return 'STATUS_UNKNOWN';
}

function randomStatus() {
  const r = Math.random() * 100;
  if (r < 78) return DeviceStatus.STATUS_ON;
  if (r < 90) return DeviceStatus.STATUS_OFF;
  return DeviceStatus.STATUS_ERROR;
}

function buildFleet() {
  for (let idx = 0; idx < FLOOD_DEVICE_COUNT; idx++) {
    const [sectorName, slug] = SECTORS[idx % SECTORS.length];
    const ordinal = Math.floor(idx / SECTORS.length) + 1;
    const deviceId = `enchente_${slug}_${String(ordinal).padStart(2, '0')}`;
    let cx = 0, cy = 0;
    if (slug === 'pici') { cx = randInt(0, 40); cy = randInt(0, 60); }
    else if (slug === 'benfica') { cx = randInt(50, 90); cy = randInt(0, 30); }
    else if (slug === 'porangabussu') { cx = randInt(60, 100); cy = randInt(50, 90); }
    else { cx = randInt(0, 30); cy = randInt(70, 100); }
    devices.set(deviceId, {
      deviceId, sector: sectorName,
      status: DeviceStatus.STATUS_ON,
      frequencySecs: 5,
      nextSendAt: 0,
      manualUntil: 0,
      lastThresholdSend: 0,
      coordX: cx, coordY: cy,
    });
  }
}

function buildFloodMetrics() {
  const waterLevel = 20 + Math.random() * 180;        // 20–200 cm (às vezes > 150)
  const flowRate = Math.random() * 50;                // 0–50 L/s
  return [
    { name: 'water_level', value: waterLevel, unit: 'cm' },
    { name: 'flow_rate', value: flowRate, unit: 'L/s' },
  ];
}

function floodThresholdReason(metrics) {
  const wl = metrics.find((m) => m.name === 'water_level');
  if (wl && wl.value >= WATER_LEVEL_THRESHOLD) {
    return `water_level=${wl.value.toFixed(1)} >= ${WATER_LEVEL_THRESHOLD}`;
  }
  return null;
}

function sendUdp(buf, port) {
  if (!txSock) return;
  txSock.send(buf, port, bestAggregatorIp, (err) => {
    if (err) console.error(`[sensor_enchente] | [UDP:Erro] porta ${port}: ${err.message}`);
  });
}

function sendDiscovery(targetDeviceId) {
  const ids = targetDeviceId ? [targetDeviceId] : Array.from(devices.keys());
  for (const id of ids) {
    const d = devices.get(id);
    if (!d) continue;
    const msg = DiscoveryResponse.create({
      messageId: `DISC-${id}-${nowSecs()}`,
      timestamp: nowSecs(),
      deviceId: id,
      type: DeviceType.DEVICE_TYPE_FLOOD,
      ipAddress: DEVICE_IP,
      controlPort: CONTROL_TCP_PORT,
      initialStatus: d.status,
      isControllable: true,
      coordX: d.coordX, coordY: d.coordY,
    });
    sendUdp(DiscoveryResponse.encode(msg).finish(), GATEWAY_DISCOVERY_PORT);
  }
}

function emitTelemetry(d, metrics, triggerReason) {
  const payload = DataPayload.create({
    messageId: `${d.deviceId}-${nowSecs()}-${randInt(1000, 9999)}`,
    timestamp: nowSecs(),
    deviceId: d.deviceId,
    currentStatus: d.status,
    metrics: metrics,
    coordX: d.coordX, coordY: d.coordY,
  });
  sendUdp(DataPayload.encode(payload).finish(), GATEWAY_TELEMETRY_PORT);
  if (d.status === DeviceStatus.STATUS_ON && metrics.length) {
    const wl = metrics.find((m) => m.name === 'water_level');
    const fr = metrics.find((m) => m.name === 'flow_rate');
    console.log(
      `[sensor_enchente] | [UDP] ${triggerReason ? 'Evento por limiar' : 'Telemetria injetada'} | ` +
      `Dispositivo=${d.deviceId} | Setor=${d.sector} | Status=${statusName(d.status)} | ` +
      `Nivel=${wl ? wl.value.toFixed(1) : '?'}cm | Vazao=${fr ? fr.value.toFixed(1) : '?'}L/s` +
      (triggerReason ? ` | Limiar=${triggerReason}` : ''));
  } else if (d.status !== DeviceStatus.STATUS_ON) {
    console.log(`[sensor_enchente] | [UDP] Heartbeat | Dispositivo=${d.deviceId} | Status=${statusName(d.status)}`);
  }
}

function authenticate() {
  return new Promise((resolve) => {
    const sock = net.createConnection({ host: GATEWAY_HOST, port: AUTH_TCP_PORT });
    let buf = Buffer.alloc(0);
    let expected = null;
    sock.setTimeout(10000);
    sock.on('connect', () => {
      const defId = Array.from(devices.keys())[0];
      const req = AuthRequest.create({
        deviceId: defId, type: DeviceType.DEVICE_TYPE_FLOOD,
        licenseKeyPart: SENSOR_LICENSE_PART, hexServiceCode: SENSOR_HEX_CODE,
      });
      const body = Buffer.from(AuthRequest.encode(req).finish());
      const header = Buffer.alloc(4); header.writeUInt32BE(body.length, 0);
      sock.write(Buffer.concat([header, body]));
    });
    sock.on('data', (chunk) => {
      buf = Buffer.concat([buf, chunk]);
      if (expected === null && buf.length >= 4) { expected = buf.readUInt32BE(0); buf = buf.subarray(4); }
      if (expected !== null && buf.length >= expected) {
        try {
          const resp = AuthResponse.decode(buf.subarray(0, expected));
          sock.destroy();
          if (!resp.success) {
            console.error(`[sensor_enchente] | [Auth] FALHA: ${resp.message}`);
            process.exit(1);
          }
          console.log(`[Auth] Gateway encontrado! Tipo: sensor_enchente, Chave: '${SENSOR_LICENSE_PART}-${SENSOR_HEX_CODE}'. Validação: SUCESSO. Porta alocada e conectada: ${toNum(resp.assignedPort)}.`);
          resolve(toNum(resp.assignedPort));
        } catch (e) {
          sock.destroy();
          console.error(`[sensor_enchente] | [Auth] Resposta inválida: ${e.message}`);
          process.exit(1);
        }
      }
    });
    sock.on('timeout', () => { sock.destroy(); console.error('[sensor_enchente] | [Auth] Timeout'); process.exit(1); });
    sock.on('error', (e) => { console.error(`[sensor_enchente] | [Auth] Erro: ${e.message}`); process.exit(1); });
  });
}

function handleControlConnection(sock) {
  let buf = Buffer.alloc(0);
  let expected = null;
  sock.setTimeout(5000);

  function sendResponse(obj) {
    let body = Buffer.from(ConfigResponse.encode(ConfigResponse.create(obj)).finish());
    if (cc.SECURE) body = cc.wrap(body);
    const header = Buffer.alloc(4); header.writeUInt32BE(body.length, 0);
    sock.end(Buffer.concat([header, body]));
  }

  sock.on('data', (chunk) => {
    buf = Buffer.concat([buf, chunk]);
    if (expected === null && buf.length >= 4) {
      expected = buf.readUInt32BE(0);
      if (expected <= 0 || expected > MAX_TCP_FRAME_BYTES) { sock.destroy(); return; }
      buf = buf.subarray(4);
    }
    if (expected === null || buf.length < expected) return;

    let body = buf.subarray(0, expected);
    try {
      if (cc.SECURE) body = cc.unwrap(body);
      const cmd = ConfigCommand.decode(body);
      if (cc.SECURE) {
        const [ok, reason] = replayGuard.check(cmd.commandId, toNum(cmd.timestamp));
        if (!ok) {
          console.error(`[sensor_enchente] | [TCP] Comando rejeitado (anti-replay): ${reason}`);
          sendResponse({ commandId: cmd.commandId, success: false, message: `Comando rejeitado (anti-replay): ${reason}` });
          return;
        }
      }
      const targetId = cmd.targetDeviceId || Array.from(devices.keys())[0];
      const d = devices.get(targetId);
      if (!d) {
        sendResponse({ commandId: cmd.commandId, success: false, message: `Dispositivo alvo desconhecido: ${targetId}` });
        return;
      }
      if (cmd.updateStatus) {
        d.status = cmd.targetStatus;
        d.manualUntil = monoSecs() + MANUAL_OVERRIDE_SECS;
      }
      if (cmd.updateFrequency && toNum(cmd.newFrequencySecs) > 0) {
        d.frequencySecs = toNum(cmd.newFrequencySecs);
      }
      d.nextSendAt = 0;
      console.log(`[sensor_enchente] | [TCP] Comando ${cmd.commandId} | Dispositivo=${targetId} | Status=${statusName(d.status)} | Frequencia=${d.frequencySecs}s`);
      sendResponse({
        messageId: `ACK-${Date.now()}`, commandId: cmd.commandId, timestamp: nowSecs(),
        success: true, message: `Sensor de enchente ${targetId} reconfigurado com sucesso.`,
        updatedStatus: d.status, updatedFrequencySecs: d.frequencySecs,
      });
      sendDiscovery(targetId);
    } catch (e) {
      console.error(`[sensor_enchente] | [TCP:Erro] ${e.message}`);
      try { sendResponse({ success: false, message: `Falha ao aplicar comando: ${e.message}` }); } catch (_) {}
    }
  });
  sock.on('timeout', () => sock.destroy());
  sock.on('error', () => {});
}

function startControlServer() {
  const server = net.createServer(handleControlConnection);
  server.on('error', (e) => console.error(`[sensor_enchente] | [TCP:Erro] servidor: ${e.message}`));
  server.listen(CONTROL_TCP_PORT, '0.0.0.0', () => {
    console.log(`[sensor_enchente] | [TCP] Interface de controle ativa na porta ${CONTROL_TCP_PORT}.`);
  });
  return server;
}

function startMulticastListener() {
  const sock = dgram.createSocket({ type: 'udp4', reuseAddr: true });
  sock.on('error', (e) => { if (!shuttingDown) console.error(`[sensor_enchente] | [Multicast:Erro] ${e.message}`); });
  sock.on('message', (msg) => {
    let load;
    try { load = AggregatorLoad.decode(msg); } catch (e) { return; }
    if (load.aggregatorId === 'GATEWAY_PROBE') {
      const jitter = Math.random() * 2000;
      setTimeout(() => { if (!shuttingDown) sendDiscovery(); }, jitter);
      return;
    }
    if (!load.ipAddress) return;
    const score = load.cpuLoad * 0.4 + toNum(load.queueSize) * 0.6;
    if (score < bestAggregatorScore || bestAggregatorIp === load.ipAddress) {
      if (bestAggregatorIp !== load.ipAddress) {
        console.log(`[sensor_enchente] | [LoadBalancer] Rota -> ${load.aggregatorId} (Score: ${bestAggregatorScore.toFixed(2)} -> ${score.toFixed(2)})`);
        bestAggregatorIp = load.ipAddress;
      }
      bestAggregatorScore = score;
    }
  });
  sock.bind(MULTICAST_PORT, () => {
    try { sock.addMembership(MULTICAST_GROUP); } catch (e) { console.error(`[sensor_enchente] | [Multicast] addMembership falhou: ${e.message}`); }
    console.log(`[sensor_enchente] | [Multicast] Escutando ${MULTICAST_GROUP}:${MULTICAST_PORT}.`);
  });
  return sock;
}

function telemetryTick() {
  if (shuttingDown) return;
  const now = monoSecs();
  for (const d of devices.values()) {
    // Limiar (evento imediato com cooldown)
    if (d.status === DeviceStatus.STATUS_ON && (now - d.lastThresholdSend) >= THRESHOLD_EVENT_COOLDOWN_SECS) {
      const m = buildFloodMetrics();
      const reason = floodThresholdReason(m);
      if (reason) { d.lastThresholdSend = now; emitTelemetry(d, m, reason); }
    }
    // Telemetria periódica
    if (now >= d.nextSendAt) {
      if (now >= d.manualUntil) d.status = randomStatus();
      d.nextSendAt = now + d.frequencySecs + Math.random() * 0.35;
      const metrics = d.status === DeviceStatus.STATUS_ON ? buildFloodMetrics() : [];
      emitTelemetry(d, metrics, null);
    }
  }
}

function startHeartbeat() {
  function schedule() {
    const delay = (HEARTBEAT_INTERVAL_SECS + Math.random() * HEARTBEAT_JITTER_SECS) * 1000;
    setTimeout(() => {
      if (shuttingDown) return;
      console.log('[sensor_enchente] | [Heartbeat] Renovando presença da frota via DiscoveryResponse.');
      sendDiscovery();
      schedule();
    }, delay);
  }
  schedule();
}

async function main() {
  await loadProto();
  DEVICE_IP = await getLocalIp();
  buildFleet();

  console.log('============================================================');
  console.log(`[sensor_enchente] | Inicializando frota de ${devices.size} sensor(es) de enchente.`);
  for (const d of devices.values()) console.log(`[sensor_enchente] | Dispositivo=${d.deviceId} | Setor=${d.sector}`);
  console.log('[sensor_enchente] | Metricas: water_level (cm), flow_rate (L/s)');
  console.log('============================================================');

  txSock = dgram.createSocket('udp4');
  txSock.on('error', () => {});

  startMulticastListener();

  console.log('[sensor_enchente] | Aguardando broadcast de AggregatorLoad para descobrir IP real...');
  while (bestAggregatorIp === GATEWAY_HOST && !shuttingDown) {
    await new Promise((r) => setTimeout(r, 100));
  }
  if (shuttingDown) return;

  sendDiscovery();
  GATEWAY_TELEMETRY_PORT = await authenticate();
  await new Promise((r) => setTimeout(r, 2000));

  startControlServer();
  startHeartbeat();

  // Console IDLE embutido (opt-in)
  if (['1', 'true', 'yes', 'on'].includes(String(process.env.SENSOR_IDLE_CONSOLE || '').toLowerCase())) {
    try {
      require('./console').startEmbedded(CONTROL_TCP_PORT);
      console.log("[sensor_enchente] | [IDLE] Console embutido ativo (use 'docker attach').");
    } catch (e) { console.error(`[sensor_enchente] | [IDLE] Falha ao iniciar console: ${e.message}`); }
  }

  setInterval(telemetryTick, 200);
}

function shutdown() {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log('\n[sensor_enchente] | Sinal recebido. Encerrando...');
  try { if (txSock) txSock.close(); } catch (e) {}
  setTimeout(() => process.exit(0), 200);
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

main().catch((e) => { console.error(`[sensor_enchente] | Erro fatal: ${e.message}`); process.exit(1); });
