#![cfg(unix)]

use prost::Message;
use sensor_water::proto::{ConfigCommand, ConfigResponse, DataPayload, DiscoveryResponse};
use sensor_water::{epoch_secs, message_id, read_frame, write_frame};
use std::collections::HashMap;
use std::net::{TcpListener, TcpStream, UdpSocket};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

struct RunningSensor(Child);

impl Drop for RunningSensor {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn sensor_process_announces_nine_devices_sends_metrics_recovers_off_and_shuts_down() {
    let discovery = UdpSocket::bind("127.0.0.1:0").unwrap();
    let telemetry = UdpSocket::bind("127.0.0.1:0").unwrap();
    discovery
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    telemetry
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    let reserved = TcpListener::bind("127.0.0.1:0").unwrap();
    let control_address = reserved.local_addr().unwrap();
    drop(reserved);
    let binary = env!("CARGO_BIN_EXE_sensor-water");
    let mut sensor = RunningSensor(
        Command::new(binary)
            .env("GATEWAY_HOST", "127.0.0.1")
            .env("SENSOR_HOSTNAME", "sensor_agua")
            .env("RUST_WATER_DEVICE_COUNT", "9")
            .env("SENSOR_CONTROL_PORT", control_address.port().to_string())
            .env(
                "GATEWAY_DISCOVERY_PORT",
                discovery.local_addr().unwrap().port().to_string(),
            )
            .env(
                "GATEWAY_TELEMETRY_PORT",
                telemetry.local_addr().unwrap().port().to_string(),
            )
            .env("SENSOR_HEARTBEAT_INTERVAL_SECS", "1")
            .env("SENSOR_HEARTBEAT_JITTER_SECS", "0")
            .env("SENSOR_MULTICAST_ENABLED", "false")
            .stdout(Stdio::null())
            .spawn()
            .unwrap(),
    );

    let mut bytes = [0_u8; 4096];
    let mut devices = HashMap::new();
    for _ in 0..9 {
        let (length, _) = discovery.recv_from(&mut bytes).unwrap();
        let announcement = DiscoveryResponse::decode(&bytes[..length]).unwrap();
        assert_eq!(announcement.r#type, 7);
        assert_eq!(announcement.control_port, i32::from(control_address.port()));
        assert_eq!(announcement.initial_status, 1);
        devices.insert(announcement.device_id.clone(), announcement);
    }
    assert_eq!(devices.len(), 9);
    let mut measured = HashMap::new();
    for _ in 0..9 {
        let (length, _) = telemetry.recv_from(&mut bytes).unwrap();
        let payload = DataPayload::decode(&bytes[..length]).unwrap();
        assert_eq!(payload.metrics.len(), 8);
        assert_eq!(payload.current_status, 1);
        assert!(devices.contains_key(&payload.device_id));
        measured.insert(payload.device_id.clone(), payload);
    }
    assert_eq!(measured.len(), 9);
    assert!(Command::new(binary)
        .arg("healthcheck")
        .env("SENSOR_HEALTHCHECK_ADDRESS", control_address.to_string())
        .status()
        .unwrap()
        .success());

    let mut control = TcpStream::connect_timeout(&control_address, Duration::from_secs(2)).unwrap();
    control
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let command = ConfigCommand {
        command_id: message_id(),
        timestamp: epoch_secs(),
        target_device_id: "water_centro_01".into(),
        update_status: true,
        target_status: 2,
        update_frequency: true,
        new_frequency_secs: 1,
    };
    write_frame(&mut control, &command).unwrap();
    let ack = ConfigResponse::decode(read_frame(&mut control).unwrap().as_slice()).unwrap();
    assert!(ack.success);
    assert_eq!(ack.command_id, command.command_id);
    assert_eq!(ack.updated_status, 2);
    assert_eq!(ack.updated_frequency_secs, 1);

    // A heartbeat must rediscover the whole fleet after the individual change.
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut rediscovered = HashMap::new();
    while rediscovered.len() < 9 && Instant::now() < deadline {
        let (length, _) = discovery.recv_from(&mut bytes).unwrap();
        let announcement = DiscoveryResponse::decode(&bytes[..length]).unwrap();
        if announcement.device_id == command.target_device_id {
            assert_eq!(announcement.initial_status, 2);
        }
        rediscovered.insert(announcement.device_id.clone(), announcement);
    }
    assert_eq!(rediscovered.len(), 9);
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut off_payload = None;
    while off_payload.is_none() && Instant::now() < deadline {
        let (length, _) = telemetry.recv_from(&mut bytes).unwrap();
        let payload = DataPayload::decode(&bytes[..length]).unwrap();
        if payload.device_id == command.target_device_id && payload.current_status == 2 {
            off_payload = Some(payload);
        }
    }
    assert!(off_payload.unwrap().metrics.is_empty());

    assert!(Command::new("sh")
        .args(["-c", "kill -TERM \"$1\"", "signal"])
        .arg(sensor.0.id().to_string())
        .status()
        .unwrap()
        .success());
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut stopped = HashMap::new();
    while stopped.len() < 9 && Instant::now() < deadline {
        let (length, _) = discovery.recv_from(&mut bytes).unwrap();
        let announcement = DiscoveryResponse::decode(&bytes[..length]).unwrap();
        if announcement.initial_status == 2 {
            stopped.insert(announcement.device_id.clone(), announcement);
        }
    }
    assert_eq!(stopped.len(), 9);
    loop {
        if let Some(status) = sensor.0.try_wait().unwrap() {
            assert!(status.success());
            break;
        }
        assert!(
            Instant::now() < deadline,
            "sensor não encerrou após SIGTERM"
        );
        thread::sleep(Duration::from_millis(20));
    }
}
