#include <assert.h>
#include <math.h>
#include <stdio.h>
#include "aqi.h"

int main(void) {
    /* Regressão: todas essas lacunas antes produziam AQI=500. */
    assert(compute_aqi(12.05) == 50.0);
    assert(compute_aqi(35.45) == 100.0);
    assert(compute_aqi(55.45) == 150.0);
    assert(compute_aqi(150.45) == 200.0);
    assert(compute_aqi(250.45) == 300.0);
    assert(compute_aqi(350.45) == 400.0);

    assert(compute_aqi(0.0) == 0.0);
    assert(compute_aqi(-1.0) == 0.0);
    assert(compute_aqi(12.1) == 51.0);
    assert(compute_aqi(35.5) == 101.0);
    assert(compute_aqi(500.4) == 500.0);
    assert(compute_aqi(1000.0) == 500.0);
    assert(compute_aqi(INFINITY) == 500.0);
    assert(isnan(compute_aqi(NAN)));

    double previous = 0.0;
    for (int sample = 0; sample <= 50100; sample++) {
        double concentration = (double)sample / 100.0;
        double index = compute_aqi(concentration);
        assert(index >= previous);
        assert(index >= 0.0 && index <= 500.0);
        assert(index == round(index));
        /* O sensor simula somente 5–45 µg/m³: nunca AQI=500. */
        if (concentration <= 45.0) assert(index < 151.0);
        previous = index;
    }
    puts("AQI: limites, lacunas e 50.101 concentrações validados.");
    return 0;
}
