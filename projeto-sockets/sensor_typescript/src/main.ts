import { SensorRuntime, loadConfiguration } from './runtime.ts';

const sensor = new SensorRuntime(loadConfiguration());
const shutdown = (): void => {
  void sensor.stop().catch(error => {
    console.error(JSON.stringify({ sensor: 'typescript', event: 'shutdown_failed', message: String(error) }));
    process.exitCode = 1;
  });
};
process.once('SIGINT', shutdown);
process.once('SIGTERM', shutdown);
try {
  await sensor.start();
} catch (error) {
  console.error(JSON.stringify({ sensor: 'typescript', event: 'startup_failed', message: String(error) }));
  await sensor.stop();
  process.exitCode = 1;
}
