import java.util.Random;

public final class TrafficSampleTest {
    private static void check(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }

    public static void main(String[] args) {
        Random random = new Random(42);
        for (int phase = 0; phase < 90; phase++) {
            for (int queue = 0; queue <= 60; queue++) {
                TrafficSample sample = TrafficSample.sample(queue, phase, 0, random);
                check(sample.queueLength == queue, "fila alterada");
                check(sample.state >= 1 && sample.state <= 3, "código de estado inválido");
                check(sample.averageWait >= 0 && sample.averageWait <= 93, "espera inválida");
                check(sample.averageSpeed > 0 && sample.averageSpeed <= 60, "velocidade inválida");
                check(sample.roadOccupancy >= 0 && sample.roadOccupancy <= 100, "ocupação inválida");
                check(sample.pedestriansCount >= 0 && sample.pedestriansCount <= 35, "pedestres inválidos");
                check(sample.cycleDuration == 90, "ciclo inválido");
                check(sample.state == 3 ? sample.greenRemaining > 0 : sample.greenRemaining == 0,
                      "tempo verde incompatível com estado do semáforo");
            }
        }
        check(TrafficSample.sample(10, 44, 0, random).state == 3, "fim do verde");
        check(TrafficSample.sample(10, 45, 0, random).state == 2, "início do amarelo");
        check(TrafficSample.sample(10, 50, 0, random).state == 1, "início do vermelho");
        check(TrafficSample.sample(10, 90, 0, random).state == 3, "novo ciclo");
        System.out.println("Semáforos: 5.490 leituras, fila, ocupação, velocidade e ciclo validados.");
    }
}
