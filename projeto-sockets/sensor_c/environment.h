#ifndef SMARTCITY_ENVIRONMENT_H
#define SMARTCITY_ENVIRONMENT_H

#include <math.h>
#include <stdlib.h>
#include "aqi.h"

#define ENVIRONMENT_METRIC_COUNT 13

static double environment_random(void) {
    return (double)rand() / (double)RAND_MAX;
}

/* Campos na mesma ordem do catálogo transmitido. Chuva e poluição reduzem
 * a visibilidade; PM10 inclui a fração PM2.5 e nunca é menor que ela. */
static void sample_environment(double values[ENVIRONMENT_METRIC_COUNT]) {
    values[0] = 25.0 + environment_random() * 10.0;
    values[1] = 55.0 + environment_random() * 35.0;
    values[2] = 400.0 + environment_random() * 200.0;
    values[3] = 5.0 + environment_random() * 40.0;
    values[4] = values[3] + 5.0 + environment_random() * 20.0;
    values[5] = compute_aqi(values[3]);
    values[6] = environment_random() * 12.0;
    values[7] = environment_random() * 359.9;
    values[8] = 1002.0 + environment_random() * 22.0;
    values[9] = values[1] >= 75.0 && environment_random() < 0.35
              ? environment_random() * 18.0 : 0.0;
    values[10] = 40.0 + environment_random() * 45.0;
    values[11] = fmax(0.5, 20.0 - values[3] * 0.18 - values[9] * 0.4);
    values[12] = values[9] > 0.0 ? environment_random() * 3.0
                                : environment_random() * 11.0;
}

#endif
