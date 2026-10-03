#ifndef SENSOR_C_AQI_H
#define SENSOR_C_AQI_H

#include <math.h>

/* A concentração PM2.5 precisa ser truncada a uma casa decimal antes
 * de consultar as faixas; valores contínuos entre 12.0 e 12.1, por
 * exemplo, não podem cair no retorno de saturação do índice. */
static double compute_aqi(double pm25) {
    static const double c_lo[] = {  0.0,  12.1,  35.5,  55.5, 150.5, 250.5, 350.5 };
    static const double c_hi[] = { 12.0,  35.4,  55.4, 150.4, 250.4, 350.4, 500.4 };
    static const int    i_lo[] = {    0,    51,   101,   151,   201,   301,   401  };
    static const int    i_hi[] = {   50,   100,   150,   200,   300,   400,   500  };

    if (isnan(pm25)) return NAN;
    if (pm25 <= 0.0) return 0.0;
    if (pm25 > c_hi[6]) return 500.0;

    pm25 = floor(pm25 * 10.0) / 10.0;
    for (int k = 0; k < 7; k++) {
        if (pm25 >= c_lo[k] && pm25 <= c_hi[k]) {
            double index = ((double)(i_hi[k] - i_lo[k]) / (c_hi[k] - c_lo[k]))
                         * (pm25 - c_lo[k]) + i_lo[k];
            return round(index);
        }
    }
    return 500.0;
}

#endif
