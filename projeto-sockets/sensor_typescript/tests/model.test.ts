import assert from 'node:assert/strict';
import { test } from 'node:test';
import { fleet, sample, telemetry } from '../src/model.ts';
import { encodeDiscovery, encodeTelemetry } from '../src/protobuf.ts';

test('fleet has 9 unique stable devices and supports 100', () => {
  assert.equal(fleet().length, 9);
  assert.equal(new Set(fleet(100).map(device => device.id)).size, 100);
  assert.equal(fleet()[0].id, 'waste_centro_01');
  for (const count of [0, 101, 1.5, NaN]) assert.throws(() => fleet(count));
});
test('metrics remain finite and respect capacity, battery and collection invariants', () => {
  const device = fleet(1)[0];
  for (let index = 0; index < 10_000; index++) {
    const metrics = new Map(sample(device, () => 0.75).map(metric => [metric.name, metric.value]));
    assert.equal(metrics.size, 9);
    metrics.forEach(value => assert.ok(Number.isFinite(value)));
    assert.ok(device.fill >= 0 && device.fill <= 100);
    assert.ok(device.battery >= 0 && device.battery <= 100);
    assert.equal(metrics.get('waste_weight'), device.fill / 100 * device.capacityKg);
  }
  assert.ok(device.collections > 0);
});
test('telemetry identities are unique and encoding rejects invalid scalars', () => {
  const device = fleet(1)[0];
  assert.notEqual(telemetry(device).messageId, telemetry(device).messageId);
  const payload = telemetry(device);
  assert.ok(encodeTelemetry(payload).length > 0);
  payload.metrics[0].value = Infinity;
  assert.throws(() => encodeTelemetry(payload));
  assert.throws(() => encodeDiscovery(device.id, 'sensor_lixeiras', 1, -1));
});
