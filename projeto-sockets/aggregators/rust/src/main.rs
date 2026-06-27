use prost::Message;
use redis::AsyncCommands;
use redis::streams::StreamMaxlen;
use std::env;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::{Mutex, oneshot};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::UdpSocket;
use tokio::time::{sleep, Duration};
use aes_gcm::{Aes128Gcm, Key, Nonce};
use aes_gcm::aead::{Aead, KeyInit};
use rand::RngCore;

pub mod smartcity {
    include!(concat!(env!("OUT_DIR"), "/smartcity.rs"));
}

// Trimming aproximado dos streams Redis para limitar o uso de memória.
const MAX_STREAM_LEN: usize = 100_000;

/// Lê um segredo de <NAME>_FILE (Docker secret) caindo para a env var <NAME>.
fn read_secret(name: &str, default: &str) -> String {
    if let Ok(path) = env::var(format!("{}_FILE", name)) {
        if let Ok(content) = std::fs::read_to_string(&path) {
            return content.trim().to_string();
        }
    }
    env::var(name).unwrap_or_else(|_| default.to_string())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Iniciando Agregador de Alta Performance Rust (Tokio)...");

    let redis_host = env::var("REDIS_HOST").unwrap_or_else(|_| "localhost".to_string());
    let redis_port = env::var("REDIS_PORT").unwrap_or_else(|_| "6379".to_string());
    let redis_url = format!("redis://{}:{}", redis_host, redis_port);
    let client = redis::Client::open(redis_url)?;
    
    let queue_size = Arc::new(AtomicUsize::new(0));
    
    let active_ports = Arc::new(Mutex::new(HashMap::<u16, oneshot::Sender<()>>::new()));
    
    // We don't need to put 5000 and 5002 in active_ports because they are permanent
    // and we don't plan to close them dynamically via Redis.
    
    // Inicia ouvintes UDP
    let telemetry_queue_size = queue_size.clone();
    let client_clone = client.clone();
    
    let aggregator_id = env::var("AGGREGATOR_ID").unwrap_or_else(|_| "rust_tokio_1".to_string());
    
    let (tx1, rx1) = oneshot::channel();
    active_ports.lock().await.insert(5000, tx1);
    let agg_id_clone1 = aggregator_id.clone();
    tokio::spawn(async move {
        listen_udp("0.0.0.0:5000", client_clone, "telemetry_stream", telemetry_queue_size, agg_id_clone1, rx1).await;
    });

    let discovery_queue_size = queue_size.clone();
    let client_clone2 = client.clone();
    let agg_id_clone2 = aggregator_id.clone();
    let (tx2, rx2) = oneshot::channel();
    active_ports.lock().await.insert(5002, tx2);
    tokio::spawn(async move {
        listen_udp("0.0.0.0:5002", client_clone2, "discovery_stream", discovery_queue_size, agg_id_clone2, rx2).await;
    });

    let pubsub_client = client.clone();
    let pubsub_agg_id = aggregator_id.clone();
    let pubsub_active_ports = active_ports.clone();
    let pubsub_queue_size = queue_size.clone();
    let pubsub_client_clone = client.clone();
    tokio::spawn(async move {
        if let Ok(mut con) = pubsub_client.get_multiplexed_async_connection().await {
            let channel = format!("agg_control_{}", pubsub_agg_id);
            println!("Ouvindo comandos no canal de controle: {}", channel);
            loop {
                // blpop retorna uma tupla (key, value)
                let res: redis::RedisResult<Vec<String>> = con.blpop(&channel, 0).await;
                if let Ok(item) = res {
                    if item.len() == 2 {
                        let payload = &item[1];
                        if payload.starts_with("ABRIR_PORTA_UDP: ") {
                            if let Ok(port) = payload[17..].trim().parse::<u16>() {
                                let mut ports = pubsub_active_ports.lock().await;
                                if !ports.contains_key(&port) {
                                    let (tx, rx) = oneshot::channel();
                                    ports.insert(port, tx);
                                    println!("Comando recebido: abrindo nova porta UDP {}", port);
                                    let bind_addr = format!("0.0.0.0:{}", port);
                                    let client_c = pubsub_client_clone.clone();
                                    let q_size_c = pubsub_queue_size.clone();
                                    let agg_id_c = pubsub_agg_id.clone();
                                    tokio::spawn(async move {
                                        listen_udp(&bind_addr, client_c, "telemetry_stream", q_size_c, agg_id_c, rx).await;
                                    });
                                }
                            }
                        } else if payload.starts_with("FECHAR_PORTA_UDP: ") {
                            if let Ok(port) = payload[18..].trim().parse::<u16>() {
                                let mut ports = pubsub_active_ports.lock().await;
                                if let Some(tx) = ports.remove(&port) {
                                    println!("Comando recebido: fechando porta UDP {}", port);
                                    let _ = tx.send(());
                                }
                            }
                        }
                    }
                } else {
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                }
            }
        }
    });

    // Inicia Anunciador Multicast
    let multicast_group = env::var("MULTICAST_GROUP").unwrap_or_else(|_| "239.0.0.1".to_string());
    let multicast_port: u16 = env::var("MULTICAST_PORT").unwrap_or("5005".to_string()).parse()?;

    let socket = std::net::UdpSocket::bind("0.0.0.0:0")?;
    let addr: SocketAddr = format!("{}:{}", multicast_group, multicast_port).parse()?;

    let local_ip = {
        let dummy = std::net::UdpSocket::bind("0.0.0.0:0").unwrap();
        dummy.connect("gateway:5000").ok();
        dummy.local_addr().map(|a| a.ip().to_string()).unwrap_or_else(|_| "127.0.0.1".to_string())
    };

    // [Fase E] Conexão Redis p/ heartbeat de saúde (health two-way ACK).
    let mut hb_conn = client.get_multiplexed_async_connection().await.ok();

    loop {
        let now_ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
        let load = smartcity::AggregatorLoad {
            aggregator_id: aggregator_id.clone(),
            ip_address: local_ip.clone(),
            telemetry_port: 5000,
            discovery_port: 5002,
            cpu_load: 5.0, // Fixo para simplificar
            queue_size: queue_size.load(Ordering::Relaxed) as i32,
            timestamp: now_ts,
        };

        let mut buf = Vec::new();
        load.encode(&mut buf)?;
        socket.send_to(&buf, &addr)?;

        // Heartbeat de saúde no Redis (TTL 30s) — lido pelo gateway (Fase E).
        if let Some(conn) = hb_conn.as_mut() {
            let key = format!("agg_heartbeat:{}", aggregator_id);
            let _: redis::RedisResult<()> = conn.set_ex(key, now_ts, 30).await;
        }

        println!("Anunciado Load: {} itens processados no último ciclo.", load.queue_size);
        queue_size.store(0, Ordering::Relaxed); // Reseta a métrica a cada ciclo

        sleep(Duration::from_secs(2)).await;
    }
}

async fn listen_udp(
    bind_addr: &str,
    client: redis::Client,
    stream_name: &str,
    queue_counter: Arc<AtomicUsize>,
    aggregator_id: String,
    mut rx_stop: oneshot::Receiver<()>,
) {
    let socket = match UdpSocket::bind(bind_addr).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Falha ao fazer bind UDP em {}: {}", bind_addr, e);
            return;
        }
    };
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let mut buf = vec![0u8; 65535];

    println!("Escutando UDP em {}", bind_addr);
    
    let raw_key = read_secret("AES_SECRET_KEY", "SmartCityKey1234");
    let mut key_bytes = [0u8; 16];
    let bytes_to_copy = std::cmp::min(16, raw_key.len());
    key_bytes[..bytes_to_copy].copy_from_slice(&raw_key.as_bytes()[..bytes_to_copy]);
    
    let key = Key::<Aes128Gcm>::from_slice(&key_bytes);
    let cipher = Aes128Gcm::new(key);

    loop {
        tokio::select! {
            _ = &mut rx_stop => {
                println!("Encerrando listener UDP em {}", bind_addr);
                break;
            }
            recv_res = socket.recv_from(&mut buf) => {
                if let Ok((len, _addr)) = recv_res {
                    let payload = &buf[..len];
            
            // Gerar 12 bytes nonce
            let mut nonce_bytes = [0u8; 12];
            rand::thread_rng().fill_bytes(&mut nonce_bytes);
            let nonce = Nonce::from_slice(&nonce_bytes);
            
            // Criptografar AES-128 GCM
            if let Ok(mut encrypted_payload) = cipher.encrypt(nonce, payload) {
                let mut final_payload = Vec::new();
                final_payload.extend_from_slice(&nonce_bytes);
                final_payload.append(&mut encrypted_payload);
                
                // Grava no Redis Streams com MAXLEN aproximado (limita memória).
                let _: redis::RedisResult<()> = conn.xadd_maxlen(
                    stream_name,
                    StreamMaxlen::Approx(MAX_STREAM_LEN),
                    "*",
                    &[
                        ("payload", final_payload.as_slice()),
                        ("aggregator", aggregator_id.as_bytes())
                    ]
                ).await;
                
                queue_counter.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
        }
    }
}
