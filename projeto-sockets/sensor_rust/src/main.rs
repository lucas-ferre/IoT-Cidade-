use prost::Message;
use rand::Rng;
use sensor_water::proto::{ConfigCommand, ConfigResponse};
use sensor_water::{
    epoch_secs, message_id, write_frame, Fleet, CONTROL_PORT, DISCOVERY_PORT, MAX_FRAME_BYTES,
    TELEMETRY_PORT,
};
use std::env;
use std::io::{self, Read};
use std::net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream, UdpSocket};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const MAX_CLIENTS: usize = 32;
const DISCOVERY_PROBE: &[u8] = b"SMARTCITY_DISCOVERY_PROBE";

#[derive(Clone)]
struct Configuration {
    gateway_host: String,
    hostname: String,
    device_count: usize,
    control_port: u16,
    telemetry_port: u16,
    discovery_port: u16,
    heartbeat: Duration,
    heartbeat_jitter: Duration,
    discovery_jitter: Duration,
    listen_probes: bool,
}

fn bounded_env(name: &str, fallback: u64, min: u64, max: u64) -> Result<u64, String> {
    let raw = env::var(name).unwrap_or_else(|_| fallback.to_string());
    raw.trim()
        .parse::<u64>()
        .ok()
        .filter(|value| (min..=max).contains(value))
        .ok_or_else(|| format!("{name} deve ser inteiro entre {min} e {max}; recebido {raw:?}"))
}

fn seconds_env(name: &str, fallback: f64, min: f64, max: f64) -> Result<Duration, String> {
    let raw = env::var(name).unwrap_or_else(|_| fallback.to_string());
    raw.trim()
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite() && (min..=max).contains(value))
        .map(Duration::from_secs_f64)
        .ok_or_else(|| format!("{name} deve estar entre {min} e {max} segundos; recebido {raw:?}"))
}

fn nonempty_env(name: &str, fallback: &str) -> Result<String, String> {
    let raw = env::var(name).unwrap_or_else(|_| fallback.into());
    let value = raw.trim();
    if value.is_empty() {
        return Err(format!("{name} não pode ser vazio"));
    }
    Ok(value.into())
}

impl Configuration {
    fn load() -> Result<Self, String> {
        let listen_probes = match env::var("SENSOR_MULTICAST_ENABLED").as_deref() {
            Ok("0" | "false") => false,
            Ok("1" | "true") | Err(_) => true,
            _ => return Err("SENSOR_MULTICAST_ENABLED deve ser true, false, 1 ou 0".into()),
        };
        Ok(Self {
            gateway_host: nonempty_env("GATEWAY_HOST", "gateway")?,
            hostname: nonempty_env("SENSOR_HOSTNAME", "sensor_agua")?,
            device_count: bounded_env("RUST_WATER_DEVICE_COUNT", 9, 1, 100)? as usize,
            control_port: bounded_env("SENSOR_CONTROL_PORT", u64::from(CONTROL_PORT), 1, 65535)?
                as u16,
            telemetry_port: bounded_env(
                "GATEWAY_TELEMETRY_PORT",
                u64::from(TELEMETRY_PORT),
                1,
                65535,
            )? as u16,
            discovery_port: bounded_env(
                "GATEWAY_DISCOVERY_PORT",
                u64::from(DISCOVERY_PORT),
                1,
                65535,
            )? as u16,
            heartbeat: seconds_env("SENSOR_HEARTBEAT_INTERVAL_SECS", 10.0, 1.0, 3600.0)?,
            heartbeat_jitter: seconds_env("SENSOR_HEARTBEAT_JITTER_SECS", 2.0, 0.0, 60.0)?,
            discovery_jitter: seconds_env("SENSOR_DISCOVERY_JITTER_SECS", 2.0, 0.0, 30.0)?,
            listen_probes,
        })
    }
}

fn jitter(max: Duration) -> Duration {
    Duration::from_millis(rand::thread_rng().gen_range(0..=max.as_millis() as u64))
}

fn send_udp(
    socket: &UdpSocket,
    config: &Configuration,
    port: u16,
    message: &impl Message,
    stopping: &AtomicBool,
) -> io::Result<()> {
    let payload = message.encode_to_vec();
    let mut last_error = None;
    for attempt in 0..3 {
        match socket.send_to(&payload, (config.gateway_host.as_str(), port)) {
            Ok(length) if length == payload.len() => return Ok(()),
            Ok(_) => {
                last_error = Some(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "datagrama incompleto",
                ))
            }
            Err(error) => last_error = Some(error),
        }
        if attempt == 2 || stopping.load(Ordering::Acquire) {
            break;
        }
        interruptible_wait(
            Duration::from_millis((200 << attempt) + rand::thread_rng().gen_range(0..=100)),
            stopping,
        );
    }
    Err(last_error.unwrap_or_else(|| io::Error::other("falha UDP")))
}

fn announce(
    socket: &UdpSocket,
    config: &Configuration,
    fleet: &Mutex<Fleet>,
    stopping: &AtomicBool,
    target: Option<&str>,
) {
    let snapshots = fleet.lock().expect("fleet mutex poisoned").snapshots();
    for device in snapshots {
        if target.is_some_and(|id| id != device.id) {
            continue;
        }
        let discovery = device.discovery(&config.hostname, config.control_port, epoch_secs());
        if let Err(error) = send_udp(socket, config, config.discovery_port, &discovery, stopping) {
            eprintln!("descoberta de {} falhou: {error}", device.id);
        }
    }
}

fn interruptible_wait(duration: Duration, stopping: &AtomicBool) {
    let deadline = Instant::now() + duration;
    while !stopping.load(Ordering::Acquire) && Instant::now() < deadline {
        thread::sleep(
            Duration::from_millis(50).min(deadline.saturating_duration_since(Instant::now())),
        );
    }
}

fn probe_socket() -> io::Result<UdpSocket> {
    let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 5005))?;
    socket.join_multicast_v4(&Ipv4Addr::new(239, 0, 0, 1), &Ipv4Addr::UNSPECIFIED)?;
    socket.set_nonblocking(true)?;
    Ok(socket)
}

fn read_control_frame(stream: &mut TcpStream, stopping: &AtomicBool) -> io::Result<Vec<u8>> {
    // The deadline spans the entire frame, including a client trickling bytes.
    // Short reads also make shutdown responsive while an attacker is connected.
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut read_exact = |buffer: &mut [u8]| -> io::Result<()> {
        let mut offset = 0;
        while offset < buffer.len() {
            if stopping.load(Ordering::Acquire) {
                return Err(io::Error::new(io::ErrorKind::Interrupted, "encerramento"));
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "prazo do frame excedido",
                ));
            }
            stream.set_read_timeout(Some(remaining.min(Duration::from_millis(100))))?;
            match stream.read(&mut buffer[offset..]) {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "frame incompleto",
                    ))
                }
                Ok(count) => offset += count,
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::WouldBlock
                            | io::ErrorKind::TimedOut
                            | io::ErrorKind::Interrupted
                    ) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    };
    let mut header = [0_u8; 4];
    read_exact(&mut header)?;
    let size = u32::from_be_bytes(header) as usize;
    if size == 0 || size > MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "tamanho de frame inválido",
        ));
    }
    let mut payload = vec![0_u8; size];
    read_exact(&mut payload)?;
    Ok(payload)
}

fn handle_connection(
    mut stream: TcpStream,
    fleet: &Mutex<Fleet>,
    config: &Configuration,
    stopping: &AtomicBool,
) -> io::Result<()> {
    stream.set_write_timeout(Some(Duration::from_secs(2)))?;
    let frame = read_control_frame(&mut stream, stopping)?;
    if stopping.load(Ordering::Acquire) {
        return Ok(());
    }
    let (response, target) = match ConfigCommand::decode(frame.as_slice()) {
        Ok(command) => {
            let response = fleet.lock().expect("fleet mutex poisoned").apply_command(
                &command,
                epoch_secs(),
                Instant::now(),
            );
            (response, Some(command.target_device_id))
        }
        Err(error) => (
            ConfigResponse {
                message_id: message_id(),
                timestamp: epoch_secs(),
                success: false,
                message: format!("Protobuf inválido: {error}"),
                ..Default::default()
            },
            None,
        ),
    };
    let send_result = write_frame(&mut stream, &response);
    eprintln!(
        "comando={} sucesso={} {}",
        response.command_id, response.success, response.message
    );
    // Announce applied changes even if the requester disconnected before ACK.
    if response.success {
        let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0))?;
        socket.set_write_timeout(Some(Duration::from_secs(1)))?;
        announce(&socket, config, fleet, stopping, target.as_deref());
    }
    send_result
}

fn control_server(
    listener: TcpListener,
    fleet: Arc<Mutex<Fleet>>,
    config: Configuration,
    stopping: Arc<AtomicBool>,
) -> io::Result<()> {
    let active = Arc::new(AtomicUsize::new(0));
    let mut handlers: Vec<JoinHandle<()>> = Vec::new();
    let mut server_error = None;
    while !stopping.load(Ordering::Acquire) {
        match listener.accept() {
            Ok((stream, peer)) => {
                if active.load(Ordering::Acquire) >= MAX_CLIENTS {
                    eprintln!("conexão de {peer} descartada: limite de clientes");
                    continue;
                }
                active.fetch_add(1, Ordering::AcqRel);
                let active = Arc::clone(&active);
                let fleet = Arc::clone(&fleet);
                let stopping = Arc::clone(&stopping);
                let config = config.clone();
                handlers.push(thread::spawn(move || {
                    if let Err(error) = handle_connection(stream, &fleet, &config, &stopping) {
                        // A healthcheck connects and closes without sending a frame.
                        if error.kind() != io::ErrorKind::UnexpectedEof {
                            eprintln!("controle de {peer}: {error}");
                        }
                    }
                    active.fetch_sub(1, Ordering::AcqRel);
                }));
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                interruptible_wait(Duration::from_millis(50), &stopping);
            }
            Err(error) => {
                stopping.store(true, Ordering::Release);
                server_error = Some(error);
                break;
            }
        }
        let mut index = 0;
        while index < handlers.len() {
            if handlers[index].is_finished() {
                let _ = handlers.swap_remove(index).join();
            } else {
                index += 1;
            }
        }
    }
    // Read/write timeouts bound the wait for an idle or malicious client.
    for handler in handlers {
        let _ = handler.join();
    }
    server_error.map_or(Ok(()), Err)
}

fn run(config: Configuration) -> Result<(), Box<dyn std::error::Error>> {
    // Bind control before the first discovery announcement.
    let listener = TcpListener::bind((Ipv4Addr::UNSPECIFIED, config.control_port))?;
    listener.set_nonblocking(true)?;
    let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0))?;
    socket.set_write_timeout(Some(Duration::from_secs(1)))?;
    let probes = if config.listen_probes {
        match probe_socket() {
            Ok(socket) => Some(socket),
            Err(error) => {
                eprintln!("multicast indisponível ({error}); heartbeat continuará a descoberta");
                None
            }
        }
    } else {
        None
    };
    let stopping = Arc::new(AtomicBool::new(false));
    let signal_stopping = Arc::clone(&stopping);
    ctrlc::set_handler(move || signal_stopping.store(true, Ordering::Release))?;
    let fleet = Arc::new(Mutex::new(Fleet::new(config.device_count, Instant::now())));
    let worker_fleet = Arc::clone(&fleet);
    let worker_stopping = Arc::clone(&stopping);
    let worker_config = config.clone();
    let control = thread::spawn(move || {
        control_server(listener, worker_fleet, worker_config, worker_stopping)
    });
    eprintln!(
        "Rust: {} sensores de água, TCP{}, destino {}",
        config.device_count, config.control_port, config.gateway_host
    );
    announce(&socket, &config, &fleet, &stopping, None);
    let mut next_heartbeat = Instant::now() + config.heartbeat + jitter(config.heartbeat_jitter);
    let mut next_probe_reply = None;
    let mut last_probe_reply = Instant::now() - Duration::from_secs(1);
    let mut probe_buffer = [0_u8; 256];
    while !stopping.load(Ordering::Acquire) {
        let now = Instant::now();
        if let Some(probes) = &probes {
            // A bounded drain prevents probe floods from starving telemetry.
            for _ in 0..16 {
                match probes.recv_from(&mut probe_buffer) {
                    Ok((length, _)) if &probe_buffer[..length] == DISCOVERY_PROBE => {
                        if next_probe_reply.is_none()
                            && now.duration_since(last_probe_reply) >= Duration::from_secs(1)
                        {
                            next_probe_reply = Some(now + jitter(config.discovery_jitter));
                        }
                    }
                    Ok(_) => {}
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                    Err(error) => {
                        eprintln!("erro de probe: {error}");
                        break;
                    }
                }
            }
        }
        if now >= next_heartbeat || next_probe_reply.is_some_and(|deadline| now >= deadline) {
            announce(&socket, &config, &fleet, &stopping, None);
            next_heartbeat = Instant::now() + config.heartbeat + jitter(config.heartbeat_jitter);
            next_probe_reply = None;
            last_probe_reply = Instant::now();
        }
        let due = fleet
            .lock()
            .expect("fleet mutex poisoned")
            .due_snapshots(now);
        for device in due {
            if let Err(error) = send_udp(
                &socket,
                &config,
                config.telemetry_port,
                &device.payload(epoch_secs()),
                &stopping,
            ) {
                eprintln!("telemetria {}: {error}", device.id);
            }
        }
        interruptible_wait(Duration::from_millis(200), &stopping);
    }
    let control_result = control
        .join()
        .map_err(|_| io::Error::other("servidor TCP interrompido"))?;
    fleet.lock().expect("fleet mutex poisoned").mark_all_off();
    announce(&socket, &config, &fleet, &stopping, None);
    eprintln!("sensor de água encerrado; último anúncio STATUS_OFF enviado");
    control_result?;
    Ok(())
}

fn healthcheck() -> Result<(), Box<dyn std::error::Error>> {
    let port = bounded_env("SENSOR_CONTROL_PORT", u64::from(CONTROL_PORT), 1, 65535)?;
    let address =
        env::var("SENSOR_HEALTHCHECK_ADDRESS").unwrap_or_else(|_| format!("127.0.0.1:{port}"));
    let address: SocketAddr = address.parse()?;
    TcpStream::connect_timeout(&address, Duration::from_millis(1500))?;
    Ok(())
}

fn main() {
    let result = if env::args().nth(1).as_deref() == Some("healthcheck") {
        healthcheck()
    } else {
        Configuration::load()
            .map_err(|error| error.into())
            .and_then(run)
    };
    if let Err(error) = result {
        eprintln!("sensor de água: {error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sensor_water::read_frame;
    use std::io::Cursor;

    fn test_config() -> Configuration {
        Configuration {
            gateway_host: "127.0.0.1".into(),
            hostname: "sensor_agua".into(),
            device_count: 1,
            control_port: CONTROL_PORT,
            telemetry_port: TELEMETRY_PORT,
            discovery_port: DISCOVERY_PORT,
            heartbeat: Duration::from_secs(10),
            heartbeat_jitter: Duration::ZERO,
            discovery_jitter: Duration::ZERO,
            listen_probes: false,
        }
    }

    #[test]
    fn tcp_control_handles_fragmented_big_endian_protobuf_frames_and_targeted_acks() {
        use std::io::Write;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let fleet = Arc::new(Mutex::new(Fleet::new(2, Instant::now())));
        let worker_fleet = Arc::clone(&fleet);
        let stopping = AtomicBool::new(false);
        let mut config = test_config();
        let discovery = UdpSocket::bind("127.0.0.1:0").unwrap();
        discovery
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        config.discovery_port = discovery.local_addr().unwrap().port();
        let worker = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            handle_connection(stream, &worker_fleet, &config, &stopping).unwrap();
        });
        let command = ConfigCommand {
            command_id: "wire:123".into(),
            timestamp: epoch_secs(),
            update_status: true,
            target_status: 2,
            update_frequency: true,
            new_frequency_secs: 1,
            target_device_id: "water_campus_01".into(),
        };
        let mut frame = Vec::new();
        write_frame(&mut frame, &command).unwrap();
        let mut client = TcpStream::connect(address).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        for fragment in frame.chunks(3) {
            client.write_all(fragment).unwrap();
        }
        let ack = ConfigResponse::decode(read_frame(&mut client).unwrap().as_slice()).unwrap();
        assert!(ack.success);
        assert_eq!(ack.command_id, command.command_id);
        assert_eq!(ack.updated_status, 2);
        assert_eq!(ack.updated_frequency_secs, 1);
        let mut buffer = [0_u8; 2048];
        let (length, _) = discovery.recv_from(&mut buffer).unwrap();
        let announcement =
            sensor_water::proto::DiscoveryResponse::decode(&buffer[..length]).unwrap();
        assert_eq!(announcement.device_id, "water_campus_01");
        assert_eq!(announcement.initial_status, 2);
        worker.join().unwrap();
        assert_eq!(fleet.lock().unwrap().snapshots()[0].status, 1);
        assert!(read_frame(&mut Cursor::new([0, 0, 0, 0])).is_err());
    }

    #[test]
    fn heartbeat_discovery_includes_all_devices_even_when_off() {
        let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
        receiver
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
        let mut config = test_config();
        config.discovery_port = receiver.local_addr().unwrap().port();
        let fleet = Mutex::new(Fleet::new(3, Instant::now()));
        fleet.lock().unwrap().mark_all_off();
        announce(&sender, &config, &fleet, &AtomicBool::new(false), None);
        let mut buffer = [0_u8; 2048];
        for _ in 0..3 {
            let (length, _) = receiver.recv_from(&mut buffer).unwrap();
            let announcement =
                sensor_water::proto::DiscoveryResponse::decode(&buffer[..length]).unwrap();
            assert_eq!(announcement.initial_status, 2);
            assert_eq!(announcement.r#type, 7);
        }
    }

    #[test]
    fn tcp_server_stops_and_joins_idle_clients_with_bounded_read_timeouts() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let stopping = Arc::new(AtomicBool::new(false));
        let worker_stopping = Arc::clone(&stopping);
        let fleet = Arc::new(Mutex::new(Fleet::new(1, Instant::now())));
        let worker =
            thread::spawn(move || control_server(listener, fleet, test_config(), worker_stopping));
        let _idle_client = TcpStream::connect(address).unwrap();
        thread::sleep(Duration::from_millis(100));
        let start = Instant::now();
        stopping.store(true, Ordering::Release);
        assert!(worker.join().unwrap().is_ok());
        assert!(start.elapsed() < Duration::from_secs(4));
    }

    #[test]
    fn shutdown_interrupts_a_partially_received_control_frame() {
        use std::io::Write;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let stopping = Arc::new(AtomicBool::new(false));
        let worker_stopping = Arc::clone(&stopping);
        let worker = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            read_control_frame(&mut stream, &worker_stopping)
        });
        let mut client = TcpStream::connect(address).unwrap();
        client.write_all(&128_u32.to_be_bytes()).unwrap();
        client.write_all(&[8, 1]).unwrap();
        thread::sleep(Duration::from_millis(100));
        let start = Instant::now();
        stopping.store(true, Ordering::Release);
        assert_eq!(
            worker.join().unwrap().unwrap_err().kind(),
            io::ErrorKind::Interrupted
        );
        assert!(start.elapsed() < Duration::from_secs(1));
    }
}
