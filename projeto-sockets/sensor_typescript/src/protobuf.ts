/** Minimal encoder for the two outbound messages in common/messages.proto.
 * Wire compatibility is verified against Python's generated Protobuf bindings.
 * This sensor only transmits messages; it exposes no control/decoder interface.
 */
import { Buffer } from 'node:buffer';
import { randomUUID } from 'node:crypto';
import type { Metric, Telemetry } from './model.ts';

function varint(value: number | bigint): Buffer {
  let remaining = BigInt(value);
  if (remaining < 0n) throw new RangeError('negative unsigned integer');
  const bytes: number[] = [];
  do { bytes.push(Number(remaining & 127n) | (remaining > 127n ? 128 : 0)); remaining >>= 7n; } while (remaining);
  return Buffer.from(bytes);
}
function integer(field: number, value: number): Buffer {
  if (!Number.isSafeInteger(value) || value < 0) throw new RangeError('invalid integer');
  return Buffer.concat([varint(field * 8), varint(value)]);
}
function data(field: number, value: Buffer): Buffer {
  return Buffer.concat([varint(field * 8 + 2), varint(value.length), value]);
}
function text(field: number, value: string): Buffer { return data(field, Buffer.from(value, 'utf8')); }
function metric(value: Metric): Buffer {
  if (!Number.isFinite(value.value)) throw new RangeError('non-finite metric');
  const bytes = Buffer.alloc(9);
  bytes[0] = 17; // Metric.value = field 2, wire type fixed64.
  bytes.writeDoubleLE(value.value, 1);
  return Buffer.concat([text(1, value.name), bytes, text(3, value.unit)]);
}
export function encodeTelemetry(value: Telemetry): Buffer {
  return Buffer.concat([
    text(1, value.messageId), integer(2, value.timestamp), text(3, value.deviceId),
    integer(4, value.status), ...value.metrics.map(value => data(5, metric(value))),
  ]);
}
export function encodeDiscovery(deviceId: string, host: string, status = 1, now = Math.floor(Date.now() / 1000),
                                messageId = `DISC-${randomUUID()}`): Buffer {
  return Buffer.concat([
    text(1, messageId), integer(2, now), text(3, deviceId),
    integer(4, 8), text(5, host), integer(6, 0), integer(7, status), integer(8, 0),
  ]);
}
