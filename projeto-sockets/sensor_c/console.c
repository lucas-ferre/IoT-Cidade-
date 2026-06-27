/* ====================================================================
 * Console de STATUS / IDLE da Estação de Clima (C) — SOMENTE LEITURA
 *
 * A estação de clima é, por projeto, NÃO-controlável (is_controllable=false,
 * sem servidor TCP de controle). Portanto este console é apenas uma interface
 * local de inspeção (status/IDLE): lê comandos do stdin e imprime o estado
 * atual da frota e o agregador escolhido. Não altera nenhum estado.
 *
 * Modo EMBUTIDO (IDLE): sensor.c inicia console_idle_thread() em uma thread
 * quando SENSOR_IDLE_CONSOLE está ligado. Requer terminal anexado
 * (docker compose: stdin_open + tty):
 *
 *     docker attach sensor_clima          # Ctrl-P Ctrl-Q para desanexar
 *
 * Comandos: status, agg, help, quit.
 * ==================================================================== */

#include <stdio.h>
#include <string.h>
#include <strings.h>   /* strcasecmp */
#include <pthread.h>
#include <signal.h>

#include "messages.pb.h"

/* Deve casar com o DEVICE_COUNT_MAX definido em sensor.c */
#define DEVICE_COUNT_MAX 10

/* Globais com linkage externa, definidos em sensor.c */
extern volatile sig_atomic_t keep_running;
extern int                   device_count;
extern char                  global_device_ids[DEVICE_COUNT_MAX][64];
extern const char           *global_device_sectors[DEVICE_COUNT_MAX];
extern smartcity_DeviceStatus global_device_statuses[DEVICE_COUNT_MAX];
extern pthread_mutex_t       statuses_mutex;
extern char                  global_best_aggregator_ip[64];
extern double                global_best_aggregator_score;
extern pthread_mutex_t       router_mutex;

static const char *console_status_text(smartcity_DeviceStatus s) {
    switch (s) {
        case smartcity_DeviceStatus_STATUS_ON:    return "ON";
        case smartcity_DeviceStatus_STATUS_OFF:   return "OFF";
        case smartcity_DeviceStatus_STATUS_ERROR: return "ERROR";
        default:                                  return "UNKNOWN";
    }
}

static void console_print_help(void) {
    printf(
        "\nConsole (estacao de clima C) — SOMENTE LEITURA (nao-controlavel):\n"
        "  status        Lista os dispositivos e seus estados atuais.\n"
        "  agg           Mostra o agregador escolhido e o score atual.\n"
        "  help          Mostra esta ajuda.\n"
        "  quit          Encerra o console (NAO encerra o sensor; use docker stop).\n\n");
    fflush(stdout);
}

static void console_print_status(void) {
    printf("[Console C:IDLE] Frota de %d estacao(oes):\n", device_count);
    pthread_mutex_lock(&statuses_mutex);
    for (int i = 0; i < device_count && i < DEVICE_COUNT_MAX; i++) {
        printf("  - %-24s | Setor=%-12s | Status=%s\n",
               global_device_ids[i],
               global_device_sectors[i] ? global_device_sectors[i] : "?",
               console_status_text(global_device_statuses[i]));
    }
    pthread_mutex_unlock(&statuses_mutex);
    fflush(stdout);
}

static void console_print_agg(void) {
    pthread_mutex_lock(&router_mutex);
    printf("[Console C:IDLE] Agregador atual: %s (score=%.2f)\n",
           global_best_aggregator_ip, global_best_aggregator_score);
    pthread_mutex_unlock(&router_mutex);
    fflush(stdout);
}

void *console_idle_thread(void *arg) {
    (void)arg;
    char line[256];

    printf("============================================================\n");
    printf("[Console C:IDLE] Console de status ativo (somente leitura).\n");
    printf("[Console C:IDLE] Use 'docker attach'; digite 'help'.\n");
    printf("============================================================\n");
    fflush(stdout);

    while (keep_running) {
        printf("clima> ");
        fflush(stdout);

        if (fgets(line, sizeof(line), stdin) == NULL) {
            break;  /* EOF no stdin */
        }
        line[strcspn(line, "\r\n")] = '\0';

        char *cmd = line;
        while (*cmd == ' ' || *cmd == '\t') cmd++;

        if (cmd[0] == '\0') {
            continue;
        } else if (strcasecmp(cmd, "status") == 0) {
            console_print_status();
        } else if (strcasecmp(cmd, "agg") == 0) {
            console_print_agg();
        } else if (strcasecmp(cmd, "help") == 0 || strcasecmp(cmd, "?") == 0) {
            console_print_help();
        } else if (strcasecmp(cmd, "quit") == 0 || strcasecmp(cmd, "exit") == 0) {
            printf("[Console C:IDLE] Console encerrado (sensor continua rodando).\n");
            fflush(stdout);
            break;
        } else {
            printf("  Comando desconhecido: '%s'. Digite 'help'.\n", cmd);
            fflush(stdout);
        }
    }
    return NULL;
}
