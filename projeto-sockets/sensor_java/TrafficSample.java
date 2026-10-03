import java.util.Random;

/** Uma leitura coerente do ciclo: 45s verde, 5s amarelo e 40s vermelho. */
final class TrafficSample {
    final int state;
    final int queueLength;
    final double averageWait;
    final double averageSpeed;
    final double roadOccupancy;
    final int cycleDuration = 90;
    final int pedestriansCount;
    final int greenRemaining;

    private TrafficSample(int queueLength, int phase, Random random) {
        this.queueLength = Math.max(0, Math.min(60, queueLength));
        this.state = phase < 45 ? 3 : (phase < 50 ? 2 : 1);
        this.greenRemaining = this.state == 3 ? 45 - phase : 0;
        this.roadOccupancy = this.queueLength / 60.0 * 100.0;
        int signalWait = phase >= 45 ? 90 - phase : 0;
        this.averageWait = signalWait + this.queueLength * 0.8;
        double roadSpeed = Math.max(5.0, 60.0 - roadOccupancy * 0.45);
        this.averageSpeed = roadSpeed * (this.state == 3 ? 1.0 : 0.35);
        this.pedestriansCount = random.nextInt(36);
    }

    static TrafficSample sample(int queueLength, long timestamp, int deviceOffset, Random random) {
        int phase = (int)Math.floorMod(timestamp + deviceOffset, 90L);
        return new TrafficSample(queueLength, phase, random);
    }
}
