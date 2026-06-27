// ====================================================================
// Agregador de Borda (Go) — novo hop no caminho até o Gateway.
//
// 3º agregador peer (além de Rust e Java): participa do mesmo load-balancer
// multicast, recebe telemetria/descoberta via UDP, cifra (AES-128-GCM) e
// repassa aos Redis Streams; anuncia AggregatorLoad por multicast, emite
// heartbeat de saúde e abre/fecha portas UDP dinâmicas via canal de controle.
//
// Stdlib + go-redis apenas. O AggregatorLoad é codificado manualmente no
// formato wire do Protobuf (mensagem simples), evitando dependência de
// protoc-gen-go no build.
// ====================================================================

package main

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

func getSecret(name, def string) string {
	if fp := os.Getenv(name + "_FILE"); fp != "" {
		if b, err := os.ReadFile(fp); err == nil {
			return strings.TrimSpace(string(b))
		}
	}
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

var (
	redisHost      = envOr("REDIS_HOST", "redis")
	redisPort      = envOr("REDIS_PORT", "6379")
	aggregatorID   = envOr("AGGREGATOR_ID", "go_edge_1")
	multicastGroup = envOr("MULTICAST_GROUP", "239.0.0.1")
	multicastPort  = envOr("MULTICAST_PORT", "5005")
	aesKey         = deriveKey(getSecret("AES_SECRET_KEY", "SmartCityKey1234"))
	maxStreamLen   = int64(100000)

	queueSize int64

	portsMu     sync.Mutex
	activePorts = map[int]context.CancelFunc{}
)

func deriveKey(raw string) []byte {
	key := make([]byte, 16)
	copy(key, []byte(raw))
	return key
}

// --------------------------------------------------------------------
// AES-128-GCM (nonce[12] || ciphertext+tag) — mesmo formato dos demais
// --------------------------------------------------------------------

func encrypt(plaintext []byte) ([]byte, error) {
	block, err := aes.NewCipher(aesKey)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, 12)
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	ct := gcm.Seal(nil, nonce, plaintext, nil)
	return append(nonce, ct...), nil
}

// --------------------------------------------------------------------
// Codificação manual do AggregatorLoad (wire Protobuf)
// --------------------------------------------------------------------

func appendTag(b []byte, field, wire int) []byte {
	return binary.AppendUvarint(b, uint64(field)<<3|uint64(wire))
}

func appendString(b []byte, field int, s string) []byte {
	b = appendTag(b, field, 2)
	b = binary.AppendUvarint(b, uint64(len(s)))
	return append(b, s...)
}

func appendVarint(b []byte, field int, v uint64) []byte {
	b = appendTag(b, field, 0)
	return binary.AppendUvarint(b, v)
}

func appendDouble(b []byte, field int, f float64) []byte {
	b = appendTag(b, field, 1)
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], math.Float64bits(f))
	return append(b, buf[:]...)
}

func encodeAggregatorLoad(id, ip string, telPort, discPort int32, cpu float64, qsize int32, ts int64) []byte {
	var b []byte
	b = appendString(b, 1, id)
	b = appendString(b, 2, ip)
	b = appendVarint(b, 3, uint64(telPort))
	b = appendVarint(b, 4, uint64(discPort))
	b = appendDouble(b, 5, cpu)
	b = appendVarint(b, 6, uint64(qsize))
	b = appendVarint(b, 7, uint64(ts))
	return b
}

// --------------------------------------------------------------------
// Listener UDP genérico
// --------------------------------------------------------------------

func listenUDP(ctx context.Context, rdb *redis.Client, bindAddr, stream string) {
	pc, err := net.ListenPacket("udp", bindAddr)
	if err != nil {
		log.Printf("[go_agg] falha no bind UDP %s: %v", bindAddr, err)
		return
	}
	defer pc.Close()
	log.Printf("[go_agg] escutando UDP em %s -> %s", bindAddr, stream)

	go func() {
		<-ctx.Done()
		pc.Close() // desbloqueia o ReadFrom
	}()

	buf := make([]byte, 65535)
	for {
		if ctx.Err() != nil {
			return
		}
		n, addr, err := pc.ReadFrom(buf)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			continue
		}
		payload := make([]byte, n)
		copy(payload, buf[:n])

		enc, err := encrypt(payload)
		if err != nil {
			continue
		}
		_ = addr
		rdb.XAdd(context.Background(), &redis.XAddArgs{
			Stream: stream,
			MaxLen: maxStreamLen,
			Approx: true,
			Values: map[string]interface{}{
				"payload":    enc,
				"aggregator": aggregatorID,
			},
		})
		atomic.AddInt64(&queueSize, 1)
	}
}

// --------------------------------------------------------------------
// Canal de controle (ABRIR/FECHAR portas UDP dinâmicas)
// --------------------------------------------------------------------

func controlLoop(rdb *redis.Client) {
	channel := "agg_control_" + aggregatorID
	log.Printf("[go_agg] ouvindo controle em %s", channel)
	for {
		res, err := rdb.BLPop(context.Background(), 0, channel).Result()
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		if len(res) != 2 {
			continue
		}
		payload := res[1]
		if strings.HasPrefix(payload, "ABRIR_PORTA_UDP: ") {
			if port, err := strconv.Atoi(strings.TrimSpace(payload[len("ABRIR_PORTA_UDP: "):])); err == nil {
				openPort(rdb, port)
			}
		} else if strings.HasPrefix(payload, "FECHAR_PORTA_UDP: ") {
			if port, err := strconv.Atoi(strings.TrimSpace(payload[len("FECHAR_PORTA_UDP: "):])); err == nil {
				closePort(port)
			}
		}
	}
}

func openPort(rdb *redis.Client, port int) {
	portsMu.Lock()
	defer portsMu.Unlock()
	if _, ok := activePorts[port]; ok {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	activePorts[port] = cancel
	log.Printf("[go_agg] abrindo porta UDP dinâmica %d", port)
	go listenUDP(ctx, rdb, fmt.Sprintf("0.0.0.0:%d", port), "telemetry_stream")
}

func closePort(port int) {
	portsMu.Lock()
	defer portsMu.Unlock()
	if cancel, ok := activePorts[port]; ok {
		log.Printf("[go_agg] fechando porta UDP dinâmica %d", port)
		cancel()
		delete(activePorts, port)
	}
}

// --------------------------------------------------------------------
// Anúncio multicast + heartbeat de saúde
// --------------------------------------------------------------------

func localIP() string {
	conn, err := net.Dial("udp", "gateway:5000")
	if err != nil {
		return "127.0.0.1"
	}
	defer conn.Close()
	if ua, ok := conn.LocalAddr().(*net.UDPAddr); ok {
		return ua.IP.String()
	}
	return "127.0.0.1"
}

func announceLoop(rdb *redis.Client) {
	ip := localIP()
	addr, err := net.ResolveUDPAddr("udp", multicastGroup+":"+multicastPort)
	if err != nil {
		log.Printf("[go_agg] addr multicast inválido: %v", err)
		return
	}
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		log.Printf("[go_agg] falha ao abrir socket multicast: %v", err)
		return
	}
	defer conn.Close()

	for {
		ts := time.Now().Unix()
		q := int32(atomic.SwapInt64(&queueSize, 0))
		// cpu_load fixo 5.0 (igual aos peers Rust/Java) — fila decide a rota.
		buf := encodeAggregatorLoad(aggregatorID, ip, 5000, 5002, 5.0, q, ts)
		if _, err := conn.Write(buf); err != nil {
			log.Printf("[go_agg] erro ao anunciar: %v", err)
		}
		// Heartbeat de saúde (TTL 30s) lido pelo gateway (Fase E).
		rdb.Set(context.Background(), "agg_heartbeat:"+aggregatorID, ts, 30*time.Second)
		log.Printf("[go_agg] Load anunciado: fila=%d", q)
		time.Sleep(2 * time.Second)
	}
}

func main() {
	log.Printf("Agregador Go iniciando (id=%s, Redis %s:%s)...", aggregatorID, redisHost, redisPort)
	rdb := redis.NewClient(&redis.Options{Addr: redisHost + ":" + redisPort})
	ctx := context.Background()

	go listenUDP(ctx, rdb, "0.0.0.0:5000", "telemetry_stream")
	go listenUDP(ctx, rdb, "0.0.0.0:5002", "discovery_stream")
	go controlLoop(rdb)

	announceLoop(rdb) // bloqueia
}
