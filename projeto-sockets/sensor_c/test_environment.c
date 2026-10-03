#include <assert.h>
#include <stdio.h>
#include "environment.h"

int main(void) {
    srand(42);
    int wet_samples = 0;
    for (int sample = 0; sample < 10000; sample++) {
        double values[ENVIRONMENT_METRIC_COUNT];
        sample_environment(values);
        for (int index = 0; index < ENVIRONMENT_METRIC_COUNT; index++) {
            assert(isfinite(values[index]) && values[index] >= 0.0);
        }
        assert(values[0] >= 25.0 && values[0] <= 35.0);
        assert(values[1] >= 55.0 && values[1] <= 90.0);
        assert(values[4] >= values[3]);
        assert(values[5] == compute_aqi(values[3]));
        assert(values[6] <= 12.0 && values[7] < 360.0);
        assert(values[8] >= 1002.0 && values[8] <= 1024.0);
        assert(values[9] <= 18.0);
        assert(values[10] >= 40.0 && values[10] <= 85.0);
        assert(values[11] >= 0.5 && values[11] <= 20.0);
        assert(values[12] <= 11.0);
        if (values[9] > 0.0) {
            wet_samples++;
            assert(values[1] >= 75.0);
            assert(values[12] <= 3.0);
        }
    }
    assert(wet_samples > 0);
    puts("Ambiente: 10.000 amostras, 13 métricas e relações físicas validadas.");
    return 0;
}
