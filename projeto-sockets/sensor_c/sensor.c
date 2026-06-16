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
#include <stdbool.h>

/* Substituímos protobuf-c por nanopb */
#include "messages.pb.h"
#include <pb_encode.h>

#define GATEWAY_HOST           "gateway"
char GATEWAY_TELEMETRY_PORT[16] = "5000";
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
#define NUM_METRICS            6
#define THRESHOLD_SCAN_INTERVAL_SECS 1.0
#define THRESHOLD_EVENT_COOLDOWN_SECS 3.0
#define TEMPERATURE_THRESHOLD_C 32.0
#define PM25_THRESHOLD_UGM3 35.0
#define AQI_THRESHOLD 100.0

#define DEVICE_COUNT_MAX 10

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
int         device_count = 3;
char        global_device_ids     [DEVICE_COUNT_MAX][64];
const char *global_device_sectors [DEVICE_COUNT_MAX];
smartcity_DeviceStatus global_device_statuses[DEVICE_COUNT_MAX];
int global_device_coord_x[DEVICE_COUNT_MAX];
int global_device_coord_y[DEVICE_COUNT_MAX];
pthread_mutex_t statuses_mutex = PTHREAD_MUTEX_INITIALIZER;

char global_best_aggregator_ip[64] = GATEWAY_HOST;
double global_best_aggregator_score = 999999.0;
pthread_mutex_t router_mutex = PTHREAD_MUTEX_INITIALIZER;


double global_last_threshold_send[DEVICE_COUNT_MAX];  /* single-threaded: main apenas */

atomic_uint global_seq_counter;   /* inicializado em main() via atomic_init() */

static const char *SENSOR_SECTORS[]      = { "Pici", "Benfica", "Porangabussu", "Labomar" };
static const char *SENSOR_SECTOR_SLUGS[] = { "pici", "benfica", "porangabussu", "labomar" };
static const int   SENSOR_SECTOR_COUNT   = 4;
static const char *METRIC_NAMES[] = { "temperature", "humidity", "co2",   "pm25",   "pm10",   "aqi"   };
static const char *METRIC_UNITS[] = {          "C",       "%",   "ppm", "ug/m3", "ug/m3", "index" };

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

static double compute_aqi(double pm25) {
    static const double c_lo[] = {  0.0,  12.1,  35.5,  55.5, 150.5, 250.5, 350.5 };
    static const double c_hi[] = { 12.0,  35.4,  55.4, 150.4, 250.4, 350.4, 500.4 };
    static const int    i_lo[] = {    0,    51,   101,   151,   201,   301,   401  };
    static const int    i_hi[] = {   50,   100,   150,   200,   300,   400,   500  };

    for (int k = 0; k < 7; k++) {
        if (pm25 >= c_lo[k] && pm25 <= c_hi[k]) {
            return ((double)(i_hi[k] - i_lo[k]) / (c_hi[k] - c_lo[k]))
                   * (pm25 - c_lo[k]) + i_lo[k];
        }
    }
    return 500.0;
}

static double monotonic_seconds(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + ((double)ts.tv_nsec / 1000000000.0);
}

// ====================================================================
// UTILITÁRIOS
// ====================================================================

static void get_local_ip(char *ip_buffer, size_t size) {
    strncpy(ip_buffer, "127.0.0.1", size);
    
    struct addrinfo hints, *res;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_DGRAM;
    if (getaddrinfo(GATEWAY_HOST, "5000", &hints, &res) == 0) {
        int sock = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
        if (sock >= 0) {
            connect(sock, res->ai_addr, res->ai_addrlen);
            struct sockaddr_in local_addr;
            socklen_t addr_len = sizeof(local_addr);
            getsockname(sock, (struct sockaddr*)&local_addr, &addr_len);
            inet_ntop(AF_INET, &local_addr.sin_addr, ip_buffer, size);
            close(sock);
        }
        freeaddrinfo(res);
    }
}

static smartcity_DeviceStatus random_device_status(void) {
    int roll = rand() % 100;
    if (roll < 78) return smartcity_DeviceStatus_STATUS_ON;
    if (roll < 90) return smartcity_DeviceStatus_STATUS_OFF;
    return smartcity_DeviceStatus_STATUS_ERROR;
}

static const char *status_to_text(smartcity_DeviceStatus s) {
    switch (s) {
        case smartcity_DeviceStatus_STATUS_ON:    return "STATUS_ON";
        case smartcity_DeviceStatus_STATUS_OFF:   return "STATUS_OFF";
        case smartcity_DeviceStatus_STATUS_ERROR: return "STATUS_ERROR";
        default:                                  return "STATUS_UNKNOWN";
    }
}

static useconds_t retry_delay_usec(int attempt) {
    long cap = UDP_RETRY_MAX_USEC;
    long temp = UDP_RETRY_BASE_USEC;
    for (int i = 0; i < attempt; i++) {
        temp *= 2;
        if (temp >= cap) { temp = cap; break; }
    }
    long delay = rand() % (temp + 1);
    return (useconds_t)delay;
}

static double read_env_double(const char *name, double fallback, double min_value) {
    const char *raw = getenv(name);
    if (raw == NULL || raw[0] == '\0')
        return fallback;

    char *endptr = NULL;
    errno = 0;
    double value = strtod(raw, &endptr);
    if (errno != 0 || endptr == raw || *endptr != '\0' || value < min_value) {
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

static void populate_environment_metrics(smartcity_Metric metrics[NUM_METRICS]) {
    double temperature = 25.0 + ((double)rand() / RAND_MAX) * 10.0;
    double humidity    = 55.0 + ((double)rand() / RAND_MAX) * 35.0;
    double co2         = 400.0 + ((double)rand() / RAND_MAX) * 200.0;
    double pm25        = 5.0  + ((double)rand() / RAND_MAX) * 40.0;
    double pm10        = pm25  + 5.0 + ((double)rand() / RAND_MAX) * 20.0;
    double aqi         = compute_aqi(pm25);

    for (int i = 0; i < NUM_METRICS; i++) {
        strncpy(metrics[i].name, METRIC_NAMES[i], sizeof(metrics[i].name) - 1);
        metrics[i].name[sizeof(metrics[i].name) - 1] = '\0';
        
        strncpy(metrics[i].unit, METRIC_UNITS[i], sizeof(metrics[i].unit) - 1);
        metrics[i].unit[sizeof(metrics[i].unit) - 1] = '\0';
    }

    metrics[0].value = temperature;
    metrics[1].value = humidity;
    metrics[2].value = co2;
    metrics[3].value = pm25;
    metrics[4].value = pm10;
    metrics[5].value = aqi;
}

static const char *environment_threshold_reason(smartcity_Metric metrics[NUM_METRICS],
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
        rc = getaddrinfo(GATEWAY_HOST, port, hints, result);
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
                                    smartcity_DataPayload *payload) {
    uint8_t buffer[1024]; // Buffer fixo, nanopb dispensa malloc
    pb_ostream_t stream = pb_ostream_from_buffer(buffer, sizeof(buffer));

    int sent_ok = 0;

    pthread_mutex_lock(&router_mutex);
    struct addrinfo *target = global_gateway_telemetry_res;
    pthread_mutex_unlock(&router_mutex);

    if (pb_encode(&stream, smartcity_DataPayload_fields, payload)) {
        sent_ok = (send_udp_with_retry(global_sockfd, buffer, stream.bytes_written,
                                       target,
                                       "Telemetria") == 0);
    } else {
        fprintf(stderr, "[Sensor C:Erro] Falha de encode nanopb para telemetria: %s\n", PB_GET_ERROR(&stream));
    }

    if (sent_ok && payload->current_status == smartcity_DeviceStatus_STATUS_ON) {
        printf("[Sensor C:UDP] %s | Dispositivo=%s | Setor=%s | Status=%s | ID=%s | "
               "Temp=%.1f°C  UR=%.0f%%  CO\xE2\x82\x82=%.0fppm  "
               "PM2.5=%.1f  PM10=%.1f  AQI=%.0f%s\n",
               trigger_reason ? "Evento por limiar" : "Telemetria injetada",
               global_device_ids[device_idx], global_device_sectors[device_idx],
               status_to_text(payload->current_status), payload->message_id,
               payload->metrics[0].value, payload->metrics[1].value, payload->metrics[2].value,
               payload->metrics[3].value, payload->metrics[4].value, payload->metrics[5].value,
               trigger_reason ? " | Limiar detectado" : "");
        if (trigger_reason) {
            printf("[Sensor C:Limiar] Dispositivo=%s | %s\n",
                   global_device_ids[device_idx], trigger_reason);
        }
    } else if (sent_ok) {
        printf("[Sensor C:UDP] Heartbeat | Dispositivo=%s | Setor=%s | Status=%s\n",
               global_device_ids[device_idx], global_device_sectors[device_idx],
               status_to_text(payload->current_status));
    } else {
        fprintf(stderr, "[Sensor C:Erro] Telemetria ID=%s descartada após retries.\n",
                payload->message_id);
    }

    return sent_ok;
}

static void poll_threshold_events(void) {
    double now_mono = monotonic_seconds();

    /* global_last_threshold_send: acesso sem mutex — single-threaded (main apenas). */
    for (int device_idx = 0; device_idx < device_count; device_idx++) {
        pthread_mutex_lock(&statuses_mutex);
        int is_on = (global_device_statuses[device_idx] == smartcity_DeviceStatus_STATUS_ON);
        pthread_mutex_unlock(&statuses_mutex);

        if (!is_on)
            continue;

        if ((now_mono - global_last_threshold_send[device_idx]) < THRESHOLD_EVENT_COOLDOWN_SECS)
            continue;

        smartcity_DataPayload payload = smartcity_DataPayload_init_zero;
        
        unsigned int seq = atomic_fetch_add_explicit(&global_seq_counter, 1, memory_order_relaxed);
        time_t now = time(NULL);
        snprintf(payload.message_id, sizeof(payload.message_id),
                 "%s-%ld-%u", global_device_ids[device_idx], (long)now, seq);

        payload.timestamp = (int64_t)now;
        strncpy(payload.device_id, global_device_ids[device_idx], sizeof(payload.device_id) - 1);
        
        pthread_mutex_lock(&statuses_mutex);
        payload.current_status = global_device_statuses[device_idx];
        pthread_mutex_unlock(&statuses_mutex);

        payload.metrics_count = NUM_METRICS;
        populate_environment_metrics(payload.metrics);

        char reason[192];
        const char *trigger_reason = environment_threshold_reason(payload.metrics, reason, sizeof(reason));
        
        if (trigger_reason != NULL) {
            global_last_threshold_send[device_idx] = now_mono;
            send_environment_payload(device_idx, trigger_reason, &payload);
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
        time_t disc_now = time(NULL);

        smartcity_DiscoveryResponse disc = smartcity_DiscoveryResponse_init_zero;
        snprintf(disc.message_id, sizeof(disc.message_id),
                 "DISC-%s-%ld", global_device_ids[i], (long)disc_now);

        disc.timestamp       = (int64_t)disc_now;
        strncpy(disc.device_id, global_device_ids[i], sizeof(disc.device_id) - 1);
        disc.type            = smartcity_DeviceType_DEVICE_TYPE_WEATHER_STATION;
        strncpy(disc.ip_address, g_self_hostname, sizeof(disc.ip_address) - 1);

        pthread_mutex_lock(&statuses_mutex);
        disc.initial_status  = global_device_statuses[i];
        smartcity_DeviceStatus status = global_device_statuses[i];
        pthread_mutex_unlock(&statuses_mutex);

        disc.is_controllable = false;
        disc.control_port    = 0;
        disc.coord_x         = global_device_coord_x[i];
        disc.coord_y         = global_device_coord_y[i];

        uint8_t buffer[512]; // Nanopb: buffer estático para o handshake
        pb_ostream_t stream = pb_ostream_from_buffer(buffer, sizeof(buffer));

        if (!pb_encode(&stream, smartcity_DiscoveryResponse_fields, &disc)) {
            fprintf(stderr, "[Sensor C:Erro] Falha de encode na descoberta de %s: %s\n",
                    global_device_ids[i], PB_GET_ERROR(&stream));
            continue;
        }

        pthread_mutex_lock(&router_mutex);
        struct addrinfo *target = global_gateway_discovery_res;
        pthread_mutex_unlock(&router_mutex);

        if (send_udp_with_retry(disc_sock, buffer, stream.bytes_written,
                                target, "Descoberta") == 0) {
            printf("[Sensor C:Descoberta] Dispositivo=%s | Setor=%s | Status=%s"
                   " | Handshake emitido via porta %s.\n",
                   global_device_ids[i], global_device_sectors[i],
                   status_to_text(status), GATEWAY_DISCOVERY_PORT);
        } else {
            fprintf(stderr, "[Sensor C:Erro] Descoberta de %s descartada após retries.\n",
                    global_device_ids[i]);
        }
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
            } else {
                smartcity_AggregatorLoad loadMsg = smartcity_AggregatorLoad_init_zero;
                pb_istream_t stream = pb_istream_from_buffer((uint8_t*)buffer, n);
                if (pb_decode(&stream, smartcity_AggregatorLoad_fields, &loadMsg)) {
                    double score = (loadMsg.cpu_load * 0.4) + (loadMsg.queue_size * 0.6);
                    
                    pthread_mutex_lock(&router_mutex);
                    if (score < global_best_aggregator_score || strcmp(global_best_aggregator_ip, loadMsg.ip_address) == 0) {
                        if (strcmp(global_best_aggregator_ip, loadMsg.ip_address) != 0) {
                            printf("[Sensor C:LoadBalancer] Rota alterada para %s (Score: %.2f -> %.2f)\n", 
                                loadMsg.aggregator_id, global_best_aggregator_score, score);
                            strncpy(global_best_aggregator_ip, loadMsg.ip_address, sizeof(global_best_aggregator_ip) - 1);
                            
                            // Re-resolver DNS para o novo IP
                            struct addrinfo hints;
                            memset(&hints, 0, sizeof(hints));
                            hints.ai_family = AF_INET;
                            hints.ai_socktype = SOCK_DGRAM;
                            
                            struct addrinfo *new_tel = NULL, *new_disc = NULL;
                            if (getaddrinfo(global_best_aggregator_ip, GATEWAY_TELEMETRY_PORT, &hints, &new_tel) == 0) {
                                if (global_gateway_telemetry_res) freeaddrinfo(global_gateway_telemetry_res);
                                global_gateway_telemetry_res = new_tel;
                            }
                            if (getaddrinfo(global_best_aggregator_ip, GATEWAY_DISCOVERY_PORT, &hints, &new_disc) == 0) {
                                if (global_gateway_discovery_res) freeaddrinfo(global_gateway_discovery_res);
                                global_gateway_discovery_res = new_disc;
                            }
                        }
                        global_best_aggregator_score = score;
                    }
                    pthread_mutex_unlock(&router_mutex);
                }
            }
        }
    }

    pthread_cleanup_pop(1);
    return NULL;
}

static int authenticate_with_gateway() {
    printf("[sensor_clima] | [Auth] Iniciando autenticacao TCP com Gateway (%s:5007)...\n", GATEWAY_HOST);
    sleep(1);
    
    struct addrinfo hints, *res;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    if (getaddrinfo(GATEWAY_HOST, "5007", &hints, &res) != 0) {
        fprintf(stderr, "[sensor_clima] | [Auth] Falha no DNS.\n");
        exit(EXIT_FAILURE);
    }
    
    int sock = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    struct timeval tv = {10, 0};
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof tv);
    
    if (connect(sock, res->ai_addr, res->ai_addrlen) < 0) {
        fprintf(stderr, "[sensor_clima] | [Auth] Erro ao conectar ao Gateway.\n");
        exit(EXIT_FAILURE);
    }
    
    smartcity_AuthRequest req = smartcity_AuthRequest_init_zero;
    strncpy(req.device_id, global_device_ids[0], sizeof(req.device_id) - 1);
    req.type = smartcity_DeviceType_DEVICE_TYPE_WEATHER_STATION;
    const char *lic = getenv("SENSOR_LICENSE_PART");
    strncpy(req.license_key_part, lic ? lic : "V1-FULL", sizeof(req.license_key_part) - 1);
    strncpy(req.hex_service_code, "0C", sizeof(req.hex_service_code) - 1);
    
    uint8_t buffer[512];
    pb_ostream_t stream = pb_ostream_from_buffer(buffer, sizeof(buffer));
    pb_encode(&stream, smartcity_AuthRequest_fields, &req);
    
    uint32_t len_net = htonl((uint32_t)stream.bytes_written);
    send(sock, &len_net, 4, 0);
    send(sock, buffer, stream.bytes_written, 0);
    
    uint32_t resp_len_net;
    if (recv(sock, &resp_len_net, 4, MSG_WAITALL) != 4) {
        fprintf(stderr, "[sensor_clima] | [Auth] Falha ao receber cabecalho TCP.\n");
        exit(EXIT_FAILURE);
    }
    uint32_t resp_len = ntohl(resp_len_net);
    
    uint8_t resp_buffer[512];
    if (recv(sock, resp_buffer, resp_len, MSG_WAITALL) != resp_len) {
        fprintf(stderr, "[sensor_clima] | [Auth] Falha ao receber payload TCP.\n");
        exit(EXIT_FAILURE);
    }
    
    smartcity_AuthResponse resp = smartcity_AuthResponse_init_zero;
    pb_istream_t in_stream = pb_istream_from_buffer(resp_buffer, resp_len);
    pb_decode(&in_stream, smartcity_AuthResponse_fields, &resp);
    
    if (!resp.success) {
        fprintf(stderr, "[sensor_clima] | [Auth] FALHA na validacao: %s\n", resp.message);
        exit(EXIT_FAILURE);
    }
    
    printf("[Auth] Gateway encontrado! Tipo: sensor_clima, Chave: '%s-0C'. Validação: SUCESSO. Porta alocada e conectada: %d.\n", lic ? lic : "V1-FULL", (int)resp.assigned_port);
    
    close(sock);
    freeaddrinfo(res);
    return resp.assigned_port;
}

// ====================================================================
// PONTO DE ENTRADA
// ====================================================================

int main(void) {
    srand((unsigned int)(time(NULL) ^ getpid()));

    atomic_init(&global_seq_counter, 0);

    if (gethostname(g_self_hostname, sizeof(g_self_hostname)) != 0) {
        get_local_ip(g_self_hostname, sizeof(g_self_hostname));
        fprintf(stderr, "[Sensor C:Config] gethostname() falhou — usando fallback '%s'.\n",
                g_self_hostname);
    }
    get_local_ip(g_self_hostname, sizeof(g_self_hostname));
    printf("[Sensor C:Config] IP resolvido: '%s'.\n", g_self_hostname);

    heartbeat_interval_secs = read_env_double("SENSOR_HEARTBEAT_INTERVAL_SECS",
                                              HEARTBEAT_INTERVAL_SECS, 1.0);
    heartbeat_jitter_secs = read_env_double("SENSOR_HEARTBEAT_JITTER_SECS",
                                            HEARTBEAT_JITTER_SECS, 0.0);

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
        global_device_statuses[i] = smartcity_DeviceStatus_STATUS_ON;
        
        int cx = 0, cy = 0;
        if (strcmp(SENSOR_SECTOR_SLUGS[sector_idx], "pici") == 0) {
            cx = rand() % 41; cy = rand() % 61;
        } else if (strcmp(SENSOR_SECTOR_SLUGS[sector_idx], "benfica") == 0) {
            cx = 50 + (rand() % 41); cy = rand() % 31;
        } else if (strcmp(SENSOR_SECTOR_SLUGS[sector_idx], "porangabussu") == 0) {
            cx = 60 + (rand() % 41); cy = 50 + (rand() % 41);
        } else { // labomar
            cx = rand() % 31; cy = 70 + (rand() % 31);
        }
        global_device_coord_x[i] = cx;
        global_device_coord_y[i] = cy;

        snprintf(global_device_ids[i], sizeof(global_device_ids[i]),
                 "estacao_%s_%02d", SENSOR_SECTOR_SLUGS[sector_idx], sector_ordinal);
        printf("           [%d] Dispositivo=%-24s | Setor=%s\n",
               i + 1, global_device_ids[i], global_device_sectors[i]);
    }
    printf("           Métricas: temperatura, umidade, CO\xE2\x82\x82, PM2.5, PM10, AQI\n");
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

    int assigned_port = authenticate_with_gateway();
    snprintf(GATEWAY_TELEMETRY_PORT, sizeof(GATEWAY_TELEMETRY_PORT), "%d", assigned_port);
    
    sleep(2);
    
    if (global_gateway_telemetry_res) freeaddrinfo(global_gateway_telemetry_res);
    
    memset(&hints, 0, sizeof(hints));
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_DGRAM;
    if (resolve_gateway_with_retry(GATEWAY_TELEMETRY_PORT,
                                   &hints, &global_gateway_telemetry_res,
                                   "Telemetria") != 0) {
        fprintf(stderr, "[Sensor C:Erro] Falha ao resolver DNS de Telemetria Pos-Auth.\n");
        exit(EXIT_FAILURE);
    }

    global_sockfd = socket(global_gateway_telemetry_res->ai_family,
                           global_gateway_telemetry_res->ai_socktype,
                           global_gateway_telemetry_res->ai_protocol);
    if (global_sockfd < 0) {
        perror("[Sensor C:Erro] Falha na criação do socket de telemetria pos-auth");
        exit(EXIT_FAILURE);
    }

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
            smartcity_DeviceStatus current_status = global_device_statuses[device_idx];
            pthread_mutex_unlock(&statuses_mutex);

            smartcity_DataPayload payload = smartcity_DataPayload_init_zero;
            time_t now = time(NULL);
            unsigned int seq = atomic_fetch_add_explicit(&global_seq_counter, 1, memory_order_relaxed);
            snprintf(payload.message_id, sizeof(payload.message_id),
                     "%s-%ld-%u", global_device_ids[device_idx], (long)now, seq);

            payload.timestamp = (int64_t)now;
            strncpy(payload.device_id, global_device_ids[device_idx], sizeof(payload.device_id) - 1);
            payload.current_status = current_status;
            payload.coord_x = global_device_coord_x[device_idx];
            payload.coord_y = global_device_coord_y[device_idx];

            const char *threshold_reason = NULL;
            if (current_status == smartcity_DeviceStatus_STATUS_ON) {
                payload.metrics_count = NUM_METRICS;
                populate_environment_metrics(payload.metrics);

                char reason[192];
                threshold_reason = environment_threshold_reason(payload.metrics, reason, sizeof(reason));
                
                if (threshold_reason != NULL) {
                    /* global_last_threshold_send: single-threaded — sem mutex */
                    global_last_threshold_send[device_idx] = monotonic_seconds();
                }
            } else {
                payload.metrics_count = 0; // Desligado, não envia métricas
            }

            send_environment_payload(device_idx, threshold_reason, &payload);
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