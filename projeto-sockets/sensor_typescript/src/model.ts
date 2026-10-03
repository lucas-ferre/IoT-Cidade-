import { randomUUID } from 'node:crypto';

export type Metric = { name: string; value: number; unit: string };
export type Device = {
  id: string; fill: number; capacityKg: number; battery: number; collections: number;
};
export type Telemetry = {
  messageId: string; timestamp: number; deviceId: string; status: number; metrics: Metric[];
};
export const sectors = ['centro', 'campus', 'hospital'];
export function fleet(count = 9): Device[] {
  if (!Number.isInteger(count) || count < 1 || count > 100) throw new RangeError('device count must be 1..100');
  return Array.from({ length: count }, (_, index) => ({
    id: `waste_${sectors[index % sectors.length]}_${String(Math.floor(index / sectors.length) + 1).padStart(2, '0')}`,
    fill: 10 + (index * 7) % 65, capacityKg: 120, battery: 100, collections: 0,
  }));
}
export function sample(device: Device, random: () => number = Math.random): Metric[] {
  device.fill = Math.min(100, device.fill + random() * 4);
  if (device.fill >= 95) { device.collections++; device.fill = 5 + random() * 10; }
  device.battery = Math.max(0, device.battery - 0.005);
  const temperature = 18 + random() * 24;
  return [
    { name: 'waste_fill_level', value: device.fill, unit: '%' },
    { name: 'waste_weight', value: device.fill / 100 * device.capacityKg, unit: 'kg' },
    { name: 'bin_temperature', value: temperature, unit: 'C' },
    { name: 'bin_humidity', value: 30 + random() * 50, unit: '%' },
    { name: 'battery_level', value: device.battery, unit: '%' },
    { name: 'signal_strength', value: -95 + random() * 50, unit: 'dBm' },
    { name: 'collection_count', value: device.collections, unit: 'count' },
    { name: 'tilt_angle', value: random() * 10, unit: 'deg' },
    { name: 'fire_risk', value: Math.max(0, (temperature - 30) * 3), unit: '%' },
  ];
}
export function telemetry(device: Device, now = Math.floor(Date.now() / 1000)): Telemetry {
  return { messageId: randomUUID(), timestamp: now, deviceId: device.id, status: 1, metrics: sample(device) };
}
