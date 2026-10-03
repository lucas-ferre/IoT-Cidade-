import dgram from 'node:dgram';
import { fleet, telemetry } from './model.ts';
import { encodeDiscovery, encodeTelemetry } from './protobuf.ts';
import type { Device } from './model.ts';

export type Configuration = {
  gatewayHost: string; advertisedHost: string; deviceCount: number;
  telemetryPort: number; discoveryPort: number;
  telemetryIntervalMs: number; heartbeatIntervalMs: number;
  sendTimeoutMs: number; shutdownTimeoutMs: number;
};
type Sender = (data: Buffer, port: number, host: string, callback: (error: Error | null) => void) => void;
type Logger = (entry: Record<string, unknown>) => void;

function numberSetting(env: NodeJS.ProcessEnv, name: string, fallback: number,
                       minimum: number, maximum: number, integer = false): number {
  const raw = env[name];
  const value = raw === undefined ? fallback : Number(raw);
  if (raw?.trim() === '' || !Number.isFinite(value) || value < minimum || value > maximum
      || (integer && !Number.isInteger(value))) {
    throw new RangeError(`${name} must be ${integer ? 'an integer ' : ''}between ${minimum} and ${maximum}`);
  }
  return value;
}
function hostSetting(env: NodeJS.ProcessEnv, name: string, fallback: string): string {
  const value = (env[name] ?? fallback).trim();
  if (!/^[A-Za-z0-9._-]{1,253}$/.test(value)) throw new RangeError(`${name} must be a hostname or IPv4 address`);
  return value;
}
export function loadConfiguration(env: NodeJS.ProcessEnv = process.env): Configuration {
  return {
    gatewayHost: hostSetting(env, 'GATEWAY_HOST', 'gateway'),
    advertisedHost: hostSetting(env, 'SENSOR_HOSTNAME', 'sensor_lixeiras'),
    deviceCount: numberSetting(env, 'TS_WASTE_DEVICE_COUNT', 9, 1, 100, true),
    telemetryPort: numberSetting(env, 'GATEWAY_TELEMETRY_PORT', 5000, 1, 65535, true),
    discoveryPort: numberSetting(env, 'GATEWAY_DISCOVERY_PORT', 5002, 1, 65535, true),
    telemetryIntervalMs: numberSetting(env, 'SENSOR_TELEMETRY_INTERVAL_SECS', 5, 0.1, 3600) * 1000,
    heartbeatIntervalMs: numberSetting(env, 'SENSOR_HEARTBEAT_INTERVAL_SECS', 10, 1, 3600) * 1000,
    sendTimeoutMs: numberSetting(env, 'TS_UDP_SEND_TIMEOUT_SECS', 1, 0.05, 10) * 1000,
    shutdownTimeoutMs: numberSetting(env, 'SENSOR_SHUTDOWN_TIMEOUT_SECS', 3, 0.1, 30) * 1000,
  };
}

export class SensorRuntime {
  readonly config: Configuration;
  private readonly socket = dgram.createSocket('udp4');
  private readonly devices: Device[];
  private readonly sender: Sender;
  private readonly logger: Logger;
  private readonly timers = new Set<ReturnType<typeof setTimeout>>();
  private readonly jobs = new Set<Promise<void>>();
  private readonly pendingSends = new Set<() => void>();
  private startPromise?: Promise<void>;
  private stopPromise?: Promise<void>;
  private stopping = false;
  private closed = false;
  private bound = false;

  constructor(config: Configuration, options: { sender?: Sender; logger?: Logger } = {}) {
    this.config = config;
    this.devices = fleet(config.deviceCount);
    this.sender = options.sender ?? ((data, port, host, callback) => this.socket.send(data, port, host, callback));
    this.logger = options.logger ?? (entry => console.log(JSON.stringify({ sensor: 'typescript', ...entry })));
    this.socket.on('error', error => this.logger({ event: 'socket_error', message: error.message }));
  }

  private async send(data: Buffer, port: number, timeoutMs = this.config.sendTimeoutMs): Promise<void> {
    if (this.closed || timeoutMs <= 0) return;
    await new Promise<void>(resolve => {
      let settled = false;
      const finish = (error: Error | null): void => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        this.pendingSends.delete(cancel);
        if (error) this.logger({ event: 'send_failed', port, message: error.message });
        resolve();
      };
      const cancel = (): void => finish(new Error('UDP send canceled during shutdown'));
      const timer = setTimeout(() => finish(new Error('UDP send deadline exceeded')), timeoutMs);
      this.pendingSends.add(cancel);
      try { this.sender(data, port, this.config.gatewayHost, finish); }
      catch (error) { finish(error instanceof Error ? error : new Error(String(error))); }
    });
  }

  private async heartbeat(status = 1, deadline = Infinity): Promise<void> {
    for (const device of this.devices) {
      if (this.closed || (status === 1 && this.stopping) || Date.now() >= deadline) break;
      await this.send(encodeDiscovery(device.id, this.config.advertisedHost, status),
        this.config.discoveryPort, Math.min(this.config.sendTimeoutMs, deadline - Date.now()));
    }
  }

  private async emit(): Promise<void> {
    for (const device of this.devices) {
      if (this.stopping || this.closed) break;
      await this.send(encodeTelemetry(telemetry(device)), this.config.telemetryPort);
    }
  }

  private track(job: Promise<void>): Promise<void> {
    this.jobs.add(job);
    void job.finally(() => this.jobs.delete(job)).catch(() => {});
    return job;
  }

  // Cada próxima rodada só é agendada quando o lote anterior terminou.
  private schedule(intervalMs: number, action: () => Promise<void>): void {
    if (this.stopping) return;
    const timer = setTimeout(() => {
      this.timers.delete(timer);
      if (this.stopping) return;
      void this.track(action()).then(() => this.schedule(intervalMs, action)).catch(error => {
        this.logger({ event: 'cycle_failed', message: String(error) });
        this.schedule(intervalMs, action);
      });
    }, intervalMs);
    this.timers.add(timer);
  }

  start(): Promise<void> {
    if (this.startPromise) return this.startPromise;
    if (this.stopping) return Promise.resolve();
    this.startPromise = this.track(this.boot());
    return this.startPromise;
  }

  private async boot(): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      const onError = (error: Error): void => { this.socket.removeListener('listening', onListening); reject(error); };
      const onListening = (): void => {
        this.socket.removeListener('error', onError);
        this.bound = true;
        resolve();
      };
      this.socket.once('error', onError);
      this.socket.once('listening', onListening);
      this.socket.bind(0);
    });
    await this.heartbeat();
    await this.emit();
    this.schedule(this.config.heartbeatIntervalMs, () => this.heartbeat());
    this.schedule(this.config.telemetryIntervalMs, () => this.emit());
    if (!this.stopping) this.logger({ event: 'started', devices: this.devices.length,
      metrics_per_device: 9, gateway: this.config.gatewayHost });
  }

  stop(): Promise<void> {
    if (this.stopPromise) return this.stopPromise;
    this.stopping = true;
    for (const timer of this.timers) clearTimeout(timer);
    this.timers.clear();
    this.stopPromise = this.drainAndClose();
    return this.stopPromise;
  }

  private async drainAndClose(): Promise<void> {
    const deadline = Date.now() + this.config.shutdownTimeoutMs;
    let drainTimer: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([
        Promise.allSettled([...this.jobs]),
        new Promise(resolve => { drainTimer = setTimeout(resolve, this.config.shutdownTimeoutMs); }),
      ]);
    } finally {
      if (drainTimer) clearTimeout(drainTimer);
    }
    if (this.bound) await this.heartbeat(2, deadline);
    if (Date.now() >= deadline && this.pendingSends.size) {
      this.logger({ event: 'shutdown_deadline', pending_sends: this.pendingSends.size });
    }
    this.closed = true;
    for (const cancel of this.pendingSends) cancel();
    this.pendingSends.clear();
    await new Promise<void>(resolve => {
      try { this.socket.close(resolve); }
      catch { resolve(); }
    });
    await Promise.allSettled([...this.jobs]);
    this.logger({ event: 'stopped' });
  }
}
