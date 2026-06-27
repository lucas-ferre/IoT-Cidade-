// ====================================================================
// NetScope — Observador de rede estilo Wireshark (Go)
//
// Faz um "tap" passivo nos Redis Streams da plataforma (telemetry_stream,
// discovery_stream e packet_trace) e reconstrói, por pacote, o CAMINHO até o
// gateway central:
//
//     Sensor (origem) ──▶ Agregador (hop) ──▶ Redis ──▶ Gateway
//
// Para cada pacote exibe origem (device_id, via decifra AES-GCM + leitura do
// campo protobuf), o agregador que o repassou (hop), tipo, tamanho e horário.
// Calcula métricas: total de pacotes, pacotes/seg, bytes, tamanho médio,
// distribuição por hop/agregador e por tipo. UI web em :8090 (auto-refresh).
//
// Não decifra para atuar — apenas observa (read-only). Sem dependência de
// Protobuf gerado: extrai o device_id por varredura leve do campo (field 3).
// ====================================================================

package main

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// --------------------------------------------------------------------
// Configuração
// --------------------------------------------------------------------

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
	redisHost = envOr("REDIS_HOST", "redis")
	redisPort = envOr("REDIS_PORT", "6379")
	httpPort  = envOr("NETSCOPE_PORT", "8090")
	capacity  = 800 // pacotes mantidos no buffer circular
	aesKey    = deriveKey(getSecret("AES_SECRET_KEY", "SmartCityKey1234"))
)

func deriveKey(raw string) []byte {
	key := make([]byte, 16)
	copy(key, []byte(raw))
	return key
}

// --------------------------------------------------------------------
// Estado de captura
// --------------------------------------------------------------------

type Packet struct {
	TsMs   int64  `json:"ts_ms"`
	Type   string `json:"type"`   // telemetry | discovery
	Hop    string `json:"hop"`    // agregador que repassou
	Device string `json:"device"` // origem (device_id) se decifrável
	Bytes  int    `json:"bytes"`
}

type Capture struct {
	mu        sync.Mutex
	packets   []Packet // ring buffer (mais recentes ao fim)
	total     int64
	bytes     int64
	byType    map[string]int64
	byHop     map[string]int64
	startTime time.Time
}

func newCapture() *Capture {
	return &Capture{
		byType:    map[string]int64{},
		byHop:     map[string]int64{},
		startTime: time.Now(),
	}
}

func (c *Capture) add(p Packet) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.packets = append(c.packets, p)
	if len(c.packets) > capacity {
		c.packets = c.packets[len(c.packets)-capacity:]
	}
	c.total++
	c.bytes += int64(p.Bytes)
	c.byType[p.Type]++
	hop := p.Hop
	if hop == "" {
		hop = "desconhecido"
	}
	c.byHop[hop]++
}

func (c *Capture) snapshotPackets(limit int) []Packet {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := len(c.packets)
	if limit > 0 && n > limit {
		n = limit
	}
	out := make([]Packet, 0, n)
	// mais recentes primeiro
	for i := len(c.packets) - 1; i >= 0 && len(out) < n; i-- {
		out = append(out, c.packets[i])
	}
	return out
}

func (c *Capture) stats() map[string]interface{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	elapsed := time.Since(c.startTime).Seconds()
	rate := 0.0
	if elapsed > 0 {
		rate = float64(c.total) / elapsed
	}
	avg := 0.0
	if c.total > 0 {
		avg = float64(c.bytes) / float64(c.total)
	}
	byType := map[string]int64{}
	for k, v := range c.byType {
		byType[k] = v
	}
	byHop := map[string]int64{}
	for k, v := range c.byHop {
		byHop[k] = v
	}
	return map[string]interface{}{
		"total_packets":  c.total,
		"total_bytes":    c.bytes,
		"packets_per_s":  rate,
		"avg_bytes":      avg,
		"uptime_seconds": elapsed,
		"by_type":        byType,
		"by_hop":         byHop,
	}
}

var capt = newCapture()

// --------------------------------------------------------------------
// Decifra AES-128-GCM + leitura leve de campo Protobuf (device_id = field 3)
// --------------------------------------------------------------------

func decryptPayload(blob []byte) ([]byte, bool) {
	if len(blob) < 12+16 {
		return nil, false
	}
	block, err := aes.NewCipher(aesKey)
	if err != nil {
		return nil, false
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, false
	}
	nonce := blob[:12]
	ct := blob[12:]
	pt, err := gcm.Open(nil, nonce, ct, nil)
	if err != nil {
		return nil, false
	}
	return pt, true
}

// protoString extrai o primeiro campo string (wire type 2) de número fieldNum.
func protoString(buf []byte, fieldNum int) string {
	i := 0
	for i < len(buf) {
		tag, n := binary.Uvarint(buf[i:])
		if n <= 0 {
			return ""
		}
		i += n
		fn := int(tag >> 3)
		wt := int(tag & 7)
		switch wt {
		case 0: // varint
			_, m := binary.Uvarint(buf[i:])
			if m <= 0 {
				return ""
			}
			i += m
		case 1: // 64-bit
			i += 8
		case 5: // 32-bit
			i += 4
		case 2: // length-delimited
			l, m := binary.Uvarint(buf[i:])
			if m <= 0 {
				return ""
			}
			i += m
			end := i + int(l)
			if end > len(buf) || end < i {
				return ""
			}
			if fn == fieldNum {
				return string(buf[i:end])
			}
			i = end
		default:
			return ""
		}
	}
	return ""
}

func extractDevice(payload []byte) string {
	pt, ok := decryptPayload(payload)
	if !ok {
		return "?"
	}
	// device_id é o campo 3 tanto em DataPayload quanto em DiscoveryResponse.
	if dev := protoString(pt, 3); dev != "" {
		return dev
	}
	return "?"
}

// --------------------------------------------------------------------
// Leitura dos streams (tap)
// --------------------------------------------------------------------

func valToBytes(v interface{}) []byte {
	switch t := v.(type) {
	case string:
		return []byte(t)
	case []byte:
		return t
	default:
		return nil
	}
}

func valToString(v interface{}) string {
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	default:
		return ""
	}
}

func tsFromID(id string) int64 {
	if idx := strings.IndexByte(id, '-'); idx > 0 {
		if ms, err := strconv.ParseInt(id[:idx], 10, 64); err == nil {
			return ms
		}
	}
	return time.Now().UnixMilli()
}

func streamReader(ctx context.Context, rdb *redis.Client, stream, typeLabel string) {
	lastID := "$"
	for ctx.Err() == nil {
		res, err := rdb.XRead(ctx, &redis.XReadArgs{
			Streams: []string{stream, lastID},
			Count:   100,
			Block:   2 * time.Second,
		}).Result()
		if err != nil {
			if err == redis.Nil {
				continue
			}
			time.Sleep(time.Second)
			continue
		}
		for _, st := range res {
			for _, msg := range st.Messages {
				lastID = msg.ID
				payload := valToBytes(msg.Values["payload"])
				hop := valToString(msg.Values["aggregator"])
				p := Packet{
					TsMs:   tsFromID(msg.ID),
					Type:   typeLabel,
					Hop:    hop,
					Device: extractDevice(payload),
					Bytes:  len(payload),
				}
				capt.add(p)
			}
		}
	}
}

// --------------------------------------------------------------------
// HTTP / UI
// --------------------------------------------------------------------

func statsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(capt.stats())
}

func packetsHandler(w http.ResponseWriter, r *http.Request) {
	limit := 200
	if q := r.URL.Query().Get("limit"); q != "" {
		if n, err := strconv.Atoi(q); err == nil && n > 0 {
			limit = n
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(capt.snapshotPackets(limit))
}

func indexHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprint(w, indexHTML)
}

const indexHTML = `<!DOCTYPE html>
<html lang="pt-br"><head><meta charset="utf-8"/>
<title>NetScope — Smart City Packet Monitor</title>
<style>
 body{font-family:system-ui,Segoe UI,Arial,sans-serif;margin:0;background:#0f1419;color:#e6e6e6}
 header{background:#161b22;padding:14px 20px;border-bottom:1px solid #30363d}
 h1{margin:0;font-size:18px} .sub{color:#8b949e;font-size:12px;margin-top:4px}
 .cards{display:flex;gap:12px;flex-wrap:wrap;padding:16px 20px}
 .card{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:12px 16px;min-width:130px}
 .card .v{font-size:22px;font-weight:700} .card .l{color:#8b949e;font-size:11px;text-transform:uppercase}
 .grid{display:flex;gap:16px;padding:0 20px 20px;flex-wrap:wrap}
 .panel{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:12px 16px;flex:1;min-width:280px}
 .panel h2{font-size:13px;margin:0 0 10px;color:#58a6ff}
 table{width:100%;border-collapse:collapse;font-size:12px}
 th,td{text-align:left;padding:5px 8px;border-bottom:1px solid #21262d}
 th{color:#8b949e;font-weight:600}
 tr.telemetry td:nth-child(2){color:#3fb950} tr.discovery td:nth-child(2){color:#d29922}
 .path{font-family:monospace;font-size:13px;color:#a5d6ff;background:#0d1117;padding:10px;border-radius:6px}
 .bar{height:8px;background:#1f6feb;border-radius:4px;display:inline-block}
</style></head>
<body>
<header><h1>🛰️ NetScope — Monitor de Pacotes (estilo Wireshark)</h1>
<div class="sub">Tap passivo nos Redis Streams · caminho Sensor ▶ Agregador ▶ Gateway · atualiza a cada 2s</div></header>
<div class="cards" id="cards"></div>
<div class="grid">
 <div class="panel"><h2>Distribuição por Hop (Agregador)</h2><div id="hops"></div>
   <div class="path" id="pathdiag" style="margin-top:10px"></div></div>
 <div class="panel"><h2>Por Tipo</h2><div id="types"></div></div>
</div>
<div class="grid"><div class="panel" style="flex-basis:100%"><h2>Captura de Pacotes (mais recentes)</h2>
 <table><thead><tr><th>Horário</th><th>Tipo</th><th>Origem (device)</th><th>Hop (agregador)</th><th>Caminho</th><th>Bytes</th></tr></thead>
 <tbody id="pkts"></tbody></table></div></div>
<script>
async function tick(){
 try{
  const s=await (await fetch('api/stats')).json();
  const p=await (await fetch('api/packets?limit=120')).json();
  document.getElementById('cards').innerHTML=card('Pacotes',s.total_packets)+card('Pacotes/seg',s.packets_per_s.toFixed(2))
    +card('Bytes',s.total_bytes)+card('Tam. médio',s.avg_bytes.toFixed(0)+' B')+card('Uptime',s.uptime_seconds.toFixed(0)+'s');
  const maxHop=Math.max(1,...Object.values(s.by_hop||{}));
  document.getElementById('hops').innerHTML=Object.entries(s.by_hop||{}).map(([k,v])=>
    '<div>'+k+' — '+v+' <span class="bar" style="width:'+(180*v/maxHop)+'px"></span></div>').join('')||'<i>sem dados</i>';
  document.getElementById('types').innerHTML=Object.entries(s.by_type||{}).map(([k,v])=>'<div>'+k+': <b>'+v+'</b></div>').join('')||'<i>sem dados</i>';
  const hops=Object.keys(s.by_hop||{}).filter(h=>h&&h!=='desconhecido');
  document.getElementById('pathdiag').textContent='Sensores ▶ ['+(hops.join(' | ')||'?')+'] ▶ Redis ▶ Gateway';
  document.getElementById('pkts').innerHTML=p.map(x=>{
   const t=new Date(x.ts_ms).toLocaleTimeString();
   return '<tr class="'+x.type+'"><td>'+t+'</td><td>'+x.type+'</td><td>'+x.device+'</td><td>'+(x.hop||'?')+
     '</td><td>'+x.device+' ▶ '+(x.hop||'?')+' ▶ gateway</td><td>'+x.bytes+'</td></tr>';
  }).join('');
 }catch(e){}
}
function card(l,v){return '<div class="card"><div class="v">'+v+'</div><div class="l">'+l+'</div></div>';}
tick();setInterval(tick,2000);
</script>
</body></html>`

func main() {
	log.Printf("NetScope iniciando — Redis %s:%s, UI em :%s", redisHost, redisPort, httpPort)
	rdb := redis.NewClient(&redis.Options{Addr: redisHost + ":" + redisPort})
	ctx := context.Background()

	go streamReader(ctx, rdb, "telemetry_stream", "telemetry")
	go streamReader(ctx, rdb, "discovery_stream", "discovery")

	http.HandleFunc("/", indexHandler)
	http.HandleFunc("/api/stats", statsHandler)
	http.HandleFunc("/api/packets", packetsHandler)

	log.Fatal(http.ListenAndServe("0.0.0.0:"+httpPort, nil))
}
