#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <signal.h>
#include <pthread.h>
#include <errno.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <stdatomic.h>
#include "messages.pb-c.h"
#include "environment.h"

#define DEFAULT_GATEWAY_HOST  "gateway"
#define GATEWAY_TELEMETRY_PORT "5000"
#define GATEWAY_DISCOVERY_PORT "5002"
#define MULTICAST_GROUP        "239.0.0.1"
#define MULTICAST_PORT         5005
#define SLEEP_INTERVAL_SECS    5
#define UDP_MAX_RETRIES        3
#define UDP_RETRY_BASE_USEC    200000
#define UDP_RETRY_MAX_USEC     1500000
#define TELEMETRY_JITTER_USEC  500000
#define DISCOVERY_PROBE_JITTER_USEC 2000000
#define HEARTBEAT_INTERVAL_SECS 10.0
#define HEARTBEAT_JITTER_SECS   2.0
#define NUM_METRICS            ENVIRONMENT_METRIC_COUNT
#define THRESHOLD_SCAN_INTERVAL_SECS 1.0
#define THRESHOLD_EVENT_COOLDOWN_SECS 3.0
#define TEMPERATURE_THRESHOLD_C 32.0
#define PM25_THRESHOLD_UGM3 35.0
#define AQI_THRESHOLD 100.0

#define DEVICE_COUNT_MAX 100

// ====================================================================
// VARIÁVEIS GLOBAIS E DE ESTADO DO CICLO DE VIDA
// ====================================================================

volatile sig_atomic_t keep_running = 1;

int global_sockfd = -1;

struct addrinfo *global_gateway_telemetry_res = NULL;
struct addrinfo *global_gateway_discovery_res = NULL;
pthread_t        listener_tid;
double           heartbeat_interval_secs = HEARTBEAT_INTERVAL_SECS;
double           heartbeat_jitter_secs   = HEARTBEAT_JITTER_SECS;

/* Frota de dispositivos — tamanho máximo estático, contagem real em device_count */
int         device_count = 12;
const char *gateway_host = DEFAULT_GATEWAY_HOST;
char        global_device_ids     [DEVICE_COUNT_MAX][64];
const char *global_device_sectors [DEVICE_COUNT_MAX];
Smartcity__DeviceStatus global_device_statuses[DEVICE_COUNT_MAX];
pthread_mutex_t statuses_mutex = PTHREAD_MUTEX_INITIALIZER;


double global_last_threshold_send[DEVICE_COUNT_MAX];  /* single-threaded: main apenas */

atomic_uint global_seq_counter;   /* inicializado em main() via atomic_init() */

static const char *SENSOR_SECTORS[]      = { "Pici", "Benfica", "Porangabussu" };
static const char *SENSOR_SECTOR_SLUGS[] = { "pici", "benfica", "porangabussu" };
static const int   SENSOR_SECTOR_COUNT   = 3;
static const char *METRIC_NAMES[] = {
    "temperature", "humidity", "co2", "pm25", "pm10", "aqi",
    "wind_speed", "wind_direction", "atmospheric_pressure", "rainfall",
    "noise_level", "visibility", "uv_index"
};
static const char *METRIC_UNITS[] = {
    "C", "%", "ppm", "ug/m3", "ug/m3", "index",
    "m/s", "deg", "hPa", "mm/h", "dB", "km", "index"
};

char g_self_hostname[256];   /* inicializado em main() antes de pthread_create() */

// ====================================================================
// TRATAMENTO DE SINAIS
// ====================================================================

void handle_shutdown_signal(int sig) {
    (void)sig;
    keep_running = 0;
}

void multicast_socket_cleanup(void *arg) {
    int *sock = (int *)arg;
    if (sock && *sock >= 0) {
        close(*sock);
        printf("[Sensor C:Teardown] Socket Multicast da thread encerrado.\n");
    }
}

// ====================================================================
// CÁLCULO DE QUALIDADE DO AR (EPA PM2.5)
// ====================================================================

static double monotonic_seconds(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + ((double)ts.tv_nsec / 1000000000.0);
}

// ====================================================================
// UTILITÁRIOS
// ====================================================================

static Smartcity__DeviceStatus random_device_status(void) {
    int roll = rand() % 100;
    if (roll < 78) return SMARTCITY__DEVICE_STATUS__STATUS_ON;
    if (roll < 90) return SMARTCITY__DEVICE_STATUS__STATUS_OFF;
    return SMARTCITY__DEVICE_STATUS__STATUS_ERROR;
}

static const char *status_to_text(Smartcity__DeviceStatus s) {
    switch (s) {
        case SMARTCITY__DEVICE_STATUS__STATUS_ON:    return "STATUS_ON";
        case SMARTCITY__DEVICE_STATUS__STATUS_OFF:   return "STATUS_OFF";
        case SMARTCITY__DEVICE_STATUS__STATUS_ERROR: return "STATUS_ERROR";
        default:                                     return "STATUS_UNKNOWN";
    }
}

static useconds_t retry_delay_usec(int attempt) {
    long delay = UDP_RETRY_BASE_USEC;
    for (int i = 0; i < attempt; i++) {
        delay *= 2;
        if (delay >= UDP_RETRY_MAX_USEC) { delay = UDP_RETRY_MAX_USEC; break; }
    }
    delay += rand() % UDP_RETRY_BASE_USEC;
    return (useconds_t)delay;
}

static double read_env_double(const char *name, double fallback, double min_value) {
    const char *raw = getenv(name);
    if (raw == NULL || raw[0] == '\0')
        return fallback;

    char *endptr = NULL;
    errno = 0;
    double value = strtod(raw, &endptr);
    if (errno != 0 || endptr == raw || *endptr != '\0' || !isfinite(value) || value < min_value) {
        fprintf(stderr,
                "[Sensor C:Config] %s='%s' inválido. Usando padrão %.1fs.\n",
                name, raw, fallback);
        return fallback;
    }

    return value;
}

static double heartbeat_delay_secs(void) {
    double jitter = heartbeat_jitter_secs <= 0.0
                  ? 0.0
                  : (((double)rand() / (double)RAND_MAX) * heartbeat_jitter_secs);
    return heartbeat_interval_secs + jitter;
}

static void init_metric_descriptors(Smartcity__Metric metrics[NUM_METRICS],
                                    Smartcity__Metric *metrics_list[NUM_METRICS]) {
    for (int i = 0; i < NUM_METRICS; i++) {
        metrics[i]      = (Smartcity__Metric)SMARTCITY__METRIC__INIT;
        metrics[i].name = (char *)METRIC_NAMES[i];
        metrics[i].unit = (char *)METRIC_UNITS[i];
        metrics_list[i] = &metrics[i];
    }
}

static void populate_environment_metrics(Smartcity__Metric metrics[NUM_METRICS]) {
    double values[NUM_METRICS];
    sample_environment(values);
    for (int i = 0; i < NUM_METRICS; i++) {
        metrics[i].value = values[i];
    }
}

static const char *environment_threshold_reason(Smartcity__Metric metrics[NUM_METRICS],
                                                char *reason,
                                                size_t reason_size) {
    int written = 0;
    reason[0] = '\0';

    if (metrics[0].value >= TEMPERATURE_THRESHOLD_C) {
        written += snprintf(reason + written, reason_size - (size_t)written,
                            "temperature=%.1f >= %.1f",
                            metrics[0].value, TEMPERATURE_THRESHOLD_C);
    }
    if (metrics[3].value >= PM25_THRESHOLD_UGM3 && (size_t)written < reason_size) {
        written += snprintf(reason + written, reason_size - (size_t)written,
                            "%spm25=%.1f >= %.1f",
                            written > 0 ? "; " : "",
                            metrics[3].value, PM25_THRESHOLD_UGM3);
    }
    if (metrics[5].value >= AQI_THRESHOLD && (size_t)written < reason_size) {
        snprintf(reason + written, reason_size - (size_t)written,
                 "%saqi=%.0f >= %.0f",
                 written > 0 ? "; " : "",
                 metrics[5].value, AQI_THRESHOLD);
    }

    return reason[0] != '\0' ? reason : NULL;
}

static int resolve_gateway_with_retry(const char *port,
                                      const struct addrinfo *hints,
                                      struct addrinfo **result,
                                      const char *channel) {
    int rc = EAI_FAIL;
    for (int attempt = 0; attempt < UDP_MAX_RETRIES; attempt++) {
        rc = getaddrinfo(gateway_host, port, hints, result);
        if (rc == 0) return 0;
        fprintf(stderr, "[Sensor C:Retry] DNS %s falhou (tentativa %d/%d): %s\n",
                channel, attempt + 1, UDP_MAX_RETRIES, gai_strerror(rc));
        if (attempt < UDP_MAX_RETRIES - 1)
            usleep(retry_delay_usec(attempt));
    }
    return rc;
}

static int send_udp_with_retry(int sockfd,
                               const uint8_t *buffer,
                               size_t len,
                               struct addrinfo *target,
                               const char *channel) {
    if (target == NULL || sockfd < 0) return -1;

    for (int attempt = 0; attempt < UDP_MAX_RETRIES; attempt++) {
        ssize_t sent = sendto(sockfd, buffer, len, 0,
                              target->ai_addr, target->ai_addrlen);
        if (sent == (ssize_t)len) return 0;

        fprintf(stderr, "[Sensor C:Retry] UDP %s falhou (tentativa %d/%d): %s\n",
                channel, attempt + 1, UDP_MAX_RETRIES, strerror(errno));
        if (attempt < UDP_MAX_RETRIES - 1)
            usleep(retry_delay_usec(attempt));
    }
    return -1;
}

static int send_environment_payload(int device_idx,
                                    const char *trigger_reason,
                                    Smartcity__Metric metrics[NUM_METRICS],
                                    Smartcity__Metric *metrics_list[NUM_METRICS]) {
    Smartcity__DataPayload payload = SMARTCITY__DATA_PAYLOAD__INIT;
    char msg_id_buffer[80];
    time_t now = time(NULL);

    unsigned int seq = atomic_fetch_add_explicit(&global_seq_counter, 1,
                                                 memory_order_relaxed);
    snprintf(msg_id_buffer, sizeof(msg_id_buffer),
             "%s-%ld-%u", global_device_ids[device_idx], now, seq);

    payload.message_id     = msg_id_buffer;
    payload.timestamp      = now;
    payload.device_id      = global_device_ids[device_idx];

    pthread_mutex_lock(&statuses_mutex);
    payload.current_status = global_device_statuses[device_idx];
    pthread_mutex_unlock(&statuses_mutex);

    if (payload.current_status == SMARTCITY__DEVICE_STATUS__STATUS_ON) {
        payload.n_metrics = NUM_METRICS;
        payload.metrics   = metrics_list;
    } else {
        payload.n_metrics = 0;
        payload.metrics   = NULL;
    }

    size_t   len = smartcity__data_payload__get_packed_size(&payload);
    uint8_t *buf = malloc(len);
    int      sent_ok = 0;

    if (buf) {
        smartcity__data_payload__pack(&payload, buf);
        sent_ok = (send_udp_with_retry(global_sockfd, buf, len,
                                       global_gateway_telemetry_res,
                                       "Telemetria") == 0);
        free(buf);
    } else {
        fprintf(stderr, "[Sensor C:Erro] Falha de alocação para telemetria.\n");
    }

    if (sent_ok && payload.current_status == SMARTCITY__DEVICE_STATUS__STATUS_ON) {
        printf("[Sensor C:UDP] %s | Dispositivo=%s | Setor=%s | Status=%s | ID=%s | "
               "Temp=%.1f°C  UR=%.0f%%  CO\xE2\x82\x82=%.0fppm  "
               "PM2.5=%.1f  PM10=%.1f  AQI=%.0f%s\n",
               trigger_reason ? "Evento por limiar" : "Telemetria injetada",
               global_device_ids[device_idx], global_device_sectors[device_idx],
               status_to_text(payload.current_status), msg_id_buffer,
               metrics[0].value, metrics[1].value, metrics[2].value,
               metrics[3].value, metrics[4].value, metrics[5].value,
               trigger_reason ? " | Limiar detectado" : "");
        if (trigger_reason) {
            printf("[Sensor C:Limiar] Dispositivo=%s | %s\n",
                   global_device_ids[device_idx], trigger_reason);
        }
    } else if (sent_ok) {
        printf("[Sensor C:UDP] Heartbeat | Dispositivo=%s | Setor=%s | Status=%s\n",
               global_device_ids[device_idx], global_device_sectors[device_idx],
               status_to_text(payload.current_status));
    } else {
        fprintf(stderr, "[Sensor C:Erro] Telemetria ID=%s descartada após retries.\n",
                msg_id_buffer);
    }

    return sent_ok;
}

static void poll_threshold_events(void) {
    double now_mono = monotonic_seconds();

    /* global_last_threshold_send: acesso sem mutex — single-threaded (main apenas). */
    for (int device_idx = 0; device_idx < device_count; device_idx++) {
        pthread_mutex_lock(&statuses_mutex);
        int is_on = (global_device_statuses[device_idx] == SMARTCITY__DEVICE_STATUS__STATUS_ON);
        pthread_mutex_unlock(&statuses_mutex);

        if (!is_on)
            continue;

        if ((now_mono - global_last_threshold_send[device_idx]) < THRESHOLD_EVENT_COOLDOWN_SECS)
            continue;

        Smartcity__Metric  metrics[NUM_METRICS];
        Smartcity__Metric *metrics_list[NUM_METRICS];
        char reason[192];

        init_metric_descriptors(metrics, metrics_list);
        populate_environment_metrics(metrics);

        if (environment_threshold_reason(metrics, reason, sizeof(reason)) != NULL) {
            global_last_threshold_send[device_idx] = now_mono;
            send_environment_payload(device_idx, reason, metrics, metrics_list);
        }
    }
}

static void sleep_with_threshold_scans(unsigned int base_secs) {
    double duration = (double)base_secs + ((double)(rand() % (TELEMETRY_JITTER_USEC + 1)) / 1000000.0);
    double end_at = monotonic_seconds() + duration;
    double next_scan_at = 0.0;

    while (keep_running) {
        double now = monotonic_seconds();
        if (now >= end_at)
            break;

        if (now >= next_scan_at) {
            poll_threshold_events();
            next_scan_at = now + THRESHOLD_SCAN_INTERVAL_SECS;
        }

        double remaining = end_at - now;
        useconds_t sleep_us = (useconds_t)((remaining < 0.1 ? remaining : 0.1) * 1000000.0);
        if (sleep_us > 0)
            usleep(sleep_us);
    }
}

static useconds_t discovery_probe_jitter_usec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);

    uint64_t mixed = (uint64_t)ts.tv_nsec
                   ^ ((uint64_t)ts.tv_sec << 21)
                   ^ ((uint64_t)getpid() << 11)
                   ^ (uint64_t)(uintptr_t)pthread_self();
    return (useconds_t)(mixed % (DISCOVERY_PROBE_JITTER_USEC + 1));
}

static void wait_discovery_probe_jitter(void) {
    useconds_t delay = discovery_probe_jitter_usec();
    printf("[Sensor C:Thread] Jitter de redescoberta: %.0f ms.\n",
           (double)delay / 1000.0);
    usleep(delay);
}

// ====================================================================
// PROTOCOLO: DESCOBERTA
// ====================================================================

void send_discovery_announcement(void) {
    if (global_gateway_discovery_res == NULL) return;

    /* Socket efêmero exclusivo desta chamada — não toca global_sockfd */
    int disc_sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (disc_sock < 0) {
        perror("[Sensor C:Descoberta] Falha ao criar socket efêmero de descoberta");
        return;
    }

    for (int i = 0; i < device_count; i++) {
        char disc_msg_id[80];
        time_t disc_now = time(NULL);
        snprintf(disc_msg_id, sizeof(disc_msg_id),
                 "DISC-%s-%ld", global_device_ids[i], (long)disc_now);

        Smartcity__DiscoveryResponse disc = SMARTCITY__DISCOVERY_RESPONSE__INIT;
        disc.message_id      = disc_msg_id;
        disc.timestamp       = (int64_t)disc_now;
        disc.device_id       = global_device_ids[i];
        disc.type            = SMARTCITY__DEVICE_TYPE__DEVICE_TYPE_WEATHER_STATION;
        disc.ip_address      = g_self_hostname;    /* [Fix 3] global, não static local */

        pthread_mutex_lock(&statuses_mutex);
        disc.initial_status  = global_device_statuses[i];
        Smartcity__DeviceStatus status = global_device_statuses[i];
        pthread_mutex_unlock(&statuses_mutex);

        disc.is_controllable = 0;
        disc.control_port    = 0;

        size_t packed_size = smartcity__discovery_response__get_packed_size(&disc);
        uint8_t *buffer = malloc(packed_size);
        if (!buffer) {
            fprintf(stderr, "[Sensor C:Erro] Falha de alocação na descoberta de %s.\n",
                    global_device_ids[i]);
            continue;
        }

        smartcity__discovery_response__pack(&disc, buffer);

        if (send_udp_with_retry(disc_sock, buffer, packed_size,
                                global_gateway_discovery_res, "Descoberta") == 0) {
            printf("[Sensor C:Descoberta] Dispositivo=%s | Setor=%s | Status=%s"
                   " | Handshake emitido via porta %s.\n",
                   global_device_ids[i], global_device_sectors[i],
                   status_to_text(status), GATEWAY_DISCOVERY_PORT);
        } else {
            fprintf(stderr, "[Sensor C:Erro] Descoberta de %s descartada após retries.\n",
                    global_device_ids[i]);
        }
        free(buffer);
    }

    close(disc_sock);
}

// ====================================================================
// GOROUTINE: LISTENER MULTICAST (thread POSIX)
// ====================================================================

void *multicast_listener_thread(void *arg) {
    (void)arg;
    int mc_sock = -1;
    struct sockaddr_in mc_addr;
    char buffer[256];

    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL);

    if ((mc_sock = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
        perror("[Sensor C:Thread] Falha na alocação de socket Multicast");
        pthread_exit(NULL);
    }

    pthread_cleanup_push(multicast_socket_cleanup, &mc_sock);

    int reuse = 1;
    setsockopt(mc_sock, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    struct timeval mc_timeout = { .tv_sec = 1, .tv_usec = 0 };
    setsockopt(mc_sock, SOL_SOCKET, SO_RCVTIMEO, &mc_timeout, sizeof(mc_timeout));

    memset(&mc_addr, 0, sizeof(mc_addr));
    mc_addr.sin_family      = AF_INET;
    mc_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    mc_addr.sin_port        = htons(MULTICAST_PORT);

    if (bind(mc_sock, (struct sockaddr *)&mc_addr, sizeof(mc_addr)) < 0) {
        perror("[Sensor C:Thread] Falha no bind Multicast");
        pthread_exit(NULL);
    }

    struct ip_mreq mreq;
    mreq.imr_multiaddr.s_addr = inet_addr(MULTICAST_GROUP);
    mreq.imr_interface.s_addr = htonl(INADDR_ANY);
    setsockopt(mc_sock, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq));

    printf("[Sensor C:Thread] Escutando Multicast em %s:%d\n",
           MULTICAST_GROUP, MULTICAST_PORT);

    struct sockaddr_in sender_addr;
    socklen_t sender_len = sizeof(sender_addr);

    while (keep_running) {
        ssize_t n = recvfrom(mc_sock, buffer, sizeof(buffer) - 1, 0,
                             (struct sockaddr *)&sender_addr, &sender_len);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                continue;
            if (keep_running)
                perror("[Sensor C:Thread] Erro no recvfrom Multicast");
            break;
        }
        if (n > 0) {
            buffer[n] = '\0';
            if (strcmp(buffer, "SMARTCITY_DISCOVERY_PROBE") == 0) {
                printf("[Sensor C:Thread] Probe interceptado — re-sincronizando topologia com jitter.\n");
                wait_discovery_probe_jitter();
                send_discovery_announcement();
            }
        }
    }

    pthread_cleanup_pop(1);
    return NULL;
}

// ====================================================================
// PONTO DE ENTRADA
// ====================================================================

int main(void) {
    srand((unsigned int)(time(NULL) ^ getpid()));

    atomic_init(&global_seq_counter, 0);

    if (gethostname(g_self_hostname, sizeof(g_self_hostname)) != 0) {
        strncpy(g_self_hostname, "sensor_clima", sizeof(g_self_hostname) - 1);
        g_self_hostname[sizeof(g_self_hostname) - 1] = '\0';
        fprintf(stderr, "[Sensor C:Config] gethostname() falhou — usando fallback '%s'.\n",
                g_self_hostname);
    }
    printf("[Sensor C:Config] Hostname resolvido: '%s'.\n", g_self_hostname);

    heartbeat_interval_secs = read_env_double("SENSOR_HEARTBEAT_INTERVAL_SECS",
                                              HEARTBEAT_INTERVAL_SECS, 1.0);
    heartbeat_jitter_secs = read_env_double("SENSOR_HEARTBEAT_JITTER_SECS",
                                            HEARTBEAT_JITTER_SECS, 0.0);

    const char *env_gateway_host = getenv("GATEWAY_HOST");
    if (env_gateway_host != NULL && env_gateway_host[0] != '\0') {
        gateway_host = env_gateway_host;
    }
    const char *env_count = getenv("C_DEVICE_COUNT");
    if (env_count != NULL && env_count[0] != '\0') {
        int parsed = atoi(env_count);
        if (parsed > 0 && parsed <= DEVICE_COUNT_MAX) {
            device_count = parsed;
        } else if (parsed > DEVICE_COUNT_MAX) {
            fprintf(stderr,
                    "[Sensor C:Config] C_DEVICE_COUNT=%d excede o máximo suportado (%d)."
                    " Usando %d.\n", parsed, DEVICE_COUNT_MAX, DEVICE_COUNT_MAX);
            device_count = DEVICE_COUNT_MAX;
        } else {
            fprintf(stderr,
                    "[Sensor C:Config] C_DEVICE_COUNT='%s' inválido. Usando padrão (%d).\n",
                    env_count, device_count);
        }
    }

    printf("============================================================\n");
    printf("[Sensor C] Inicializando frota de %d Estação(ões) Ambiental(is)...\n",
           device_count);
    for (int i = 0; i < device_count; i++) {
        int sector_idx = i % SENSOR_SECTOR_COUNT;
        int sector_ordinal = (i / SENSOR_SECTOR_COUNT) + 1;
        global_device_sectors [i] = SENSOR_SECTORS[sector_idx];
        global_device_statuses[i] = SMARTCITY__DEVICE_STATUS__STATUS_ON;
        snprintf(global_device_ids[i], sizeof(global_device_ids[i]),
                 "estacao_%s_%02d", SENSOR_SECTOR_SLUGS[sector_idx], sector_ordinal);
        printf("           [%d] Dispositivo=%-24s | Setor=%s\n",
               i + 1, global_device_ids[i], global_device_sectors[i]);
    }
    printf("           %d métricas ambientais por dispositivo; destino UDP: %s\n", NUM_METRICS, gateway_host);
    printf("============================================================\n");

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = handle_shutdown_signal;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGINT,  &sa, NULL);

    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_DGRAM;

    if (resolve_gateway_with_retry(GATEWAY_TELEMETRY_PORT,
                                   &hints, &global_gateway_telemetry_res,
                                   "Telemetria") != 0) {
        fprintf(stderr, "[Sensor C:Erro] Falha ao resolver DNS de Telemetria.\n");
        exit(EXIT_FAILURE);
    }
    if (resolve_gateway_with_retry(GATEWAY_DISCOVERY_PORT,
                                   &hints, &global_gateway_discovery_res,
                                   "Descoberta") != 0) {
        fprintf(stderr, "[Sensor C:Erro] Falha ao resolver DNS de Descoberta.\n");
        freeaddrinfo(global_gateway_telemetry_res);
        exit(EXIT_FAILURE);
    }

    global_sockfd = socket(global_gateway_telemetry_res->ai_family,
                           global_gateway_telemetry_res->ai_socktype,
                           global_gateway_telemetry_res->ai_protocol);
    if (global_sockfd < 0) {
        perror("[Sensor C:Erro] Falha na criação do socket de telemetria");
        freeaddrinfo(global_gateway_telemetry_res);
        freeaddrinfo(global_gateway_discovery_res);
        exit(EXIT_FAILURE);
    }

    /* Handshake topológico inicial — g_self_hostname já está inicializado */
    send_discovery_announcement();

    /* Thread de escuta Multicast — g_self_hostname é read-only a partir daqui */
    if (pthread_create(&listener_tid, NULL, multicast_listener_thread, NULL) != 0)
        perror("[Sensor C:Aviso] Falha ao criar thread Multicast");

    double next_heartbeat_at = monotonic_seconds() + heartbeat_delay_secs();

    while (keep_running) {
        double now_mono = monotonic_seconds();
        if (now_mono >= next_heartbeat_at) {
            printf("[Sensor C:Heartbeat] Renovando presença da frota via DiscoveryResponse.\n");
            send_discovery_announcement();
            next_heartbeat_at = now_mono + heartbeat_delay_secs();
        }

        for (int device_idx = 0; device_idx < device_count; device_idx++) {
            pthread_mutex_lock(&statuses_mutex);
            global_device_statuses[device_idx] = random_device_status();
            Smartcity__DeviceStatus current_status = global_device_statuses[device_idx];
            pthread_mutex_unlock(&statuses_mutex);

            Smartcity__Metric  metrics[NUM_METRICS];
            Smartcity__Metric *metrics_list[NUM_METRICS];
            char reason[192];

            init_metric_descriptors(metrics, metrics_list);
            populate_environment_metrics(metrics);

            const char *threshold_reason = NULL;
            if (current_status == SMARTCITY__DEVICE_STATUS__STATUS_ON) {
                threshold_reason = environment_threshold_reason(
                    metrics, reason, sizeof(reason));
                if (threshold_reason != NULL) {
                    /* global_last_threshold_send: single-threaded — sem mutex */
                    global_last_threshold_send[device_idx] = monotonic_seconds();
                }
            }

            send_environment_payload(device_idx, threshold_reason, metrics, metrics_list);
        }

        sleep_with_threshold_scans(SLEEP_INTERVAL_SECS);
    }

    // ====================================================================
    // TEARDOWN
    // ====================================================================
    printf("\n[Sensor C] Sinal POSIX interceptado. Iniciando Teardown...\n");

    pthread_cancel(listener_tid);
    pthread_join(listener_tid, NULL);

    if (global_gateway_telemetry_res) {
        freeaddrinfo(global_gateway_telemetry_res);
        global_gateway_telemetry_res = NULL;
        printf("[Sensor C:Teardown] DNS Telemetria liberado.\n");
    }
    if (global_gateway_discovery_res) {
        freeaddrinfo(global_gateway_discovery_res);
        global_gateway_discovery_res = NULL;
        printf("[Sensor C:Teardown] DNS Descoberta liberado.\n");
    }
    if (global_sockfd >= 0) {
        close(global_sockfd);
        global_sockfd = -1;
        printf("[Sensor C:Teardown] Socket de telemetria encerrado.\n");
    }

    printf("[Sensor C] Encerrado com código POSIX 0.\n");
    return 0;
}
