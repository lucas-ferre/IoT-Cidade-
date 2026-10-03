import assert from 'node:assert/strict';
import dgram from 'node:dgram';
import { setTimeout as sleep } from 'node:timers/promises';
import { test } from 'node:test';
import { SensorRuntime, loadConfiguration } from '../src/runtime.ts';

async function receiver(): Promise<{ socket: dgram.Socket; port: number; packets: Buffer[] }> {
  const socket = dgram.createSocket('udp4');
  const packets: Buffer[] = [];
  socket.on('message', data => packets.push(data));
  await new Promise<void>((resolve, reject) => {
    socket.once('error', reject);
    socket.bind(0, '127.0.0.1', resolve);
  });
  return { socket, port: socket.address().port, packets };
}
async function waitFor(check: () => boolean, timeout = 1000): Promise<void> {
  const deadline = Date.now() + timeout;
  while (!check()) {
    if (Date.now() >= deadline) assert.fail('packet/event deadline exceeded');
    await sleep(5);
  }
}
const base = () => ({ ...loadConfiguration({}), gatewayHost: '127.0.0.1',
  telemetryIntervalMs: 20, heartbeatIntervalMs: 30, sendTimeoutMs: 100, shutdownTimeoutMs: 300 });
const quiet = (): void => {};

test('configuration rejects non-finite intervals, invalid counts/ports and blank hosts', () => {
  const valid = loadConfiguration({ GATEWAY_HOST: '127.0.0.1', GATEWAY_TELEMETRY_PORT: '54321' });
  assert.equal(valid.deviceCount, 9);
  assert.equal(valid.telemetryPort, 54321);
  for (const value of ['NaN', 'Infinity', '', '-1', '0', '101', '1.5']) {
    assert.throws(() => loadConfiguration({ TS_WASTE_DEVICE_COUNT: value }));
  }
  for (const name of ['SENSOR_HEARTBEAT_INTERVAL_SECS', 'SENSOR_TELEMETRY_INTERVAL_SECS',
                      'TS_UDP_SEND_TIMEOUT_SECS', 'SENSOR_SHUTDOWN_TIMEOUT_SECS']) {
    for (const value of ['NaN', 'Infinity', '', '-1']) assert.throws(() => loadConfiguration({ [name]: value }));
  }
  for (const value of ['0', '65536', '1.5', 'NaN']) {
    assert.throws(() => loadConfiguration({ GATEWAY_DISCOVERY_PORT: value }));
  }
  for (const value of ['', ' ', 'bad host']) assert.throws(() => loadConfiguration({ GATEWAY_HOST: value }));
});

test('real UDP sends all devices, renews discovery, and ends with OFF before closing', async t => {
  const discovery = await receiver();
  const telemetry = await receiver();
  const runtime = new SensorRuntime({ ...base(), discoveryPort: discovery.port, telemetryPort: telemetry.port }, { logger: quiet });
  t.after(async () => { await runtime.stop(); discovery.socket.close(); telemetry.socket.close(); });
  await runtime.start();
  await waitFor(() => discovery.packets.length >= 9 && telemetry.packets.length >= 9);
  await waitFor(() => telemetry.packets.length >= 18 && discovery.packets.length >= 18);
  await runtime.stop();
  await waitFor(() => discovery.packets.length >= 27);
  const finalPackets = discovery.packets.slice(-9);
  // Tags 6=control_port, 7=initial_status, 8=is_controllable are encoded last.
  assert.ok(finalPackets.every(data => data.subarray(-6).equals(Buffer.from([48, 0, 56, 2, 64, 0]))));
  assert.equal(new Set(finalPackets.map(packet => packet.toString('hex'))).size, 9);
  const packetCounts = [discovery.packets.length, telemetry.packets.length];
  await sleep(80);
  assert.deepEqual([discovery.packets.length, telemetry.packets.length], packetCounts);
});

test('shutdown during initial handshake cancels telemetry and installs no future timers', async () => {
  const sent: number[] = [];
  const runtime = new SensorRuntime(base(), { logger: quiet, sender: (_data, port, _host, callback) => {
    sent.push(port); setTimeout(() => callback(null), 15);
  } });
  const starting = runtime.start();
  await waitFor(() => sent.length >= 1);
  const stopping = runtime.stop();
  assert.equal(runtime.stop(), stopping, 'stop must be idempotent');
  await stopping;
  await starting;
  assert.ok(sent.every(port => port === 5002), 'no telemetry after early shutdown');
  const count = sent.length;
  await sleep(80);
  assert.equal(sent.length, count, 'no timer survives stop');
});

test('slow sends cannot overlap subsequent telemetry batches', async () => {
  let telemetryInFlight = 0;
  let maximumInFlight = 0;
  let telemetrySends = 0;
  const runtime = new SensorRuntime({ ...base(), deviceCount: 2, heartbeatIntervalMs: 1000, telemetryIntervalMs: 5 }, {
    logger: quiet, sender: (_data, port, _host, callback) => {
      if (port === 5000) { telemetrySends++; telemetryInFlight++; maximumInFlight = Math.max(maximumInFlight, telemetryInFlight); }
      setTimeout(() => { if (port === 5000) telemetryInFlight--; callback(null); }, 20);
    },
  });
  try {
    await runtime.start();
    await waitFor(() => telemetrySends >= 6);
  } finally { await runtime.stop(); }
  assert.equal(maximumInFlight, 1);
  assert.equal(telemetryInFlight, 0);
});

test('missing send callbacks and DNS errors are bounded, logged, and stop safely', async () => {
  const entries: Record<string, unknown>[] = [];
  const runtime = new SensorRuntime({ ...base(), deviceCount: 1, sendTimeoutMs: 20 }, {
    logger: entry => entries.push(entry), sender: (_data, port, _host, callback) => {
      if (port === 5000) callback(new Error('getaddrinfo ENOTFOUND gateway'));
    },
  });
  await runtime.start();
  const now = Date.now();
  await runtime.stop();
  assert.ok(Date.now() - now < 400);
  assert.ok(entries.some(entry => String(entry.message).includes('deadline')));
  assert.ok(entries.some(entry => String(entry.message).includes('ENOTFOUND')));
});

test('shutdown deadline cancels pending send timers even when their timeout is much longer', async () => {
  const entries: Record<string, unknown>[] = [];
  let sends = 0;
  const runtime = new SensorRuntime({ ...base(), sendTimeoutMs: 5000, shutdownTimeoutMs: 80 }, {
    logger: entry => entries.push(entry), sender: () => { sends++; },
  });
  const starting = runtime.start();
  await waitFor(() => sends === 1);
  const began = Date.now();
  await runtime.stop();
  await starting;
  assert.ok(Date.now() - began < 400, 'long send timer cannot prolong shutdown');
  assert.equal(sends, 1);
  assert.ok(entries.some(entry => entry.event === 'shutdown_deadline'));
  const eventCount = entries.length;
  await sleep(80);
  assert.equal(entries.length, eventCount, 'no delayed callback or timer survives close');
});
