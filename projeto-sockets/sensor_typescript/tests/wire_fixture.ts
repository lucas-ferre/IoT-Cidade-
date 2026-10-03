import { encodeDiscovery, encodeTelemetry } from '../src/protobuf.ts';
import type { Telemetry } from '../src/model.ts';

const data: Telemetry = {
  messageId: 'fixture-telemetry', timestamp: 1099511627776, deviceId: 'waste_centro_01', status: 1,
  metrics: [
    { name: 'waste_fill_level', value: 42.5, unit: '%' },
    { name: 'signal_strength', value: -72.25, unit: 'dBm' },
    { name: 'unicode_á', value: 0.125, unit: 'µg/m³' },
  ],
};
console.log(JSON.stringify({
  discovery: encodeDiscovery(data.deviceId, 'sensor_lixeiras', 1, data.timestamp, 'fixture-discovery').toString('hex'),
  telemetry: encodeTelemetry(data).toString('hex'),
}));
