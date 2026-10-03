use prost::Message;
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/smartcity.rs"));
}

use proto::{
    ConfigCommand, ConfigResponse, DataPayload, DeviceStatus, DeviceType, DiscoveryResponse, Metric,
};

pub const CONTROL_PORT: u16 = 5008;
pub const TELEMETRY_PORT: u16 = 5000;
pub const DISCOVERY_PORT: u16 = 5002;
pub const WATER_SENSOR_TYPE: i32 = DeviceType::WaterSensor as i32;
pub const MAX_FRAME_BYTES: usize = 1024 * 1024;
const REPLAY_WINDOW_SECS: i64 = 600;
const MAX_RECENT_COMMANDS: usize = 4096;

pub fn epoch_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

pub fn message_id() -> String {
    Uuid::new_v4().to_string()
}

pub fn valid_command_id(id: &str) -> bool {
    let bytes = id.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 128
        && bytes[0].is_ascii_alphanumeric()
        && bytes
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || b"._:-".contains(b))
}

pub fn valid_device_id(id: &str) -> bool {
    let bytes = id.as_bytes();
    let valid_initial = |b: u8| b.is_ascii_lowercase() || b.is_ascii_digit();
    !bytes.is_empty()
        && bytes.len() <= 128
        && valid_initial(bytes[0])
        && bytes
            .iter()
            .all(|b| valid_initial(*b) || *b == b'_' || *b == b'-')
}

pub fn validate_command(command: &ConfigCommand, now: i64) -> Result<(), String> {
    if !valid_command_id(&command.command_id) {
        return Err("command_id ausente ou com formato inválido".into());
    }
    if !valid_device_id(&command.target_device_id) {
        return Err("target_device_id ausente ou com formato inválido".into());
    }
    // Subtraction in i128 also rejects crafted timestamps without overflowing.
    let age = i128::from(now) - i128::from(command.timestamp);
    if command.timestamp <= 0 || !(-60..=300).contains(&age) {
        return Err("timestamp inválido, expirado (>300s) ou futuro (>60s)".into());
    }
    if !command.update_status && !command.update_frequency {
        return Err("comando não solicita alterações".into());
    }
    if command.update_status
        && command.target_status != DeviceStatus::StatusOn as i32
        && command.target_status != DeviceStatus::StatusOff as i32
    {
        return Err("target_status deve ser STATUS_ON ou STATUS_OFF".into());
    }
    if command.update_frequency && !(1..=60).contains(&command.new_frequency_secs) {
        return Err("new_frequency_secs deve estar entre 1 e 60".into());
    }
    Ok(())
}

#[derive(Clone, Debug)]
pub struct WaterDevice {
    pub id: String,
    pub status: i32,
    pub frequency_secs: i32,
    next_send: Instant,
    level: f64,
    flow: f64,
    pressure: f64,
    ph: f64,
    turbidity: f64,
    temperature: f64,
    conductivity: f64,
    leak: f64,
}

impl WaterDevice {
    pub fn metrics(&self) -> Vec<Metric> {
        if self.status != DeviceStatus::StatusOn as i32 {
            return Vec::new();
        }
        [
            ("water_level", self.level, "%"),
            ("water_flow", self.flow, "L/min"),
            ("water_pressure", self.pressure, "bar"),
            ("water_ph", self.ph, "pH"),
            ("water_turbidity", self.turbidity, "NTU"),
            ("water_temperature", self.temperature, "°C"),
            ("conductivity", self.conductivity, "uS/cm"),
            ("leak_rate", self.leak, "L/min"),
        ]
        .into_iter()
        .map(|(name, value, unit)| Metric {
            name: name.into(),
            value,
            unit: unit.into(),
        })
        .collect()
    }

    pub fn payload(&self, timestamp: i64) -> DataPayload {
        DataPayload {
            message_id: message_id(),
            timestamp,
            device_id: self.id.clone(),
            current_status: self.status,
            metrics: self.metrics(),
        }
    }

    pub fn discovery(&self, hostname: &str, port: u16, timestamp: i64) -> DiscoveryResponse {
        DiscoveryResponse {
            message_id: message_id(),
            timestamp,
            device_id: self.id.clone(),
            r#type: WATER_SENSOR_TYPE,
            ip_address: hostname.into(),
            control_port: i32::from(port),
            initial_status: self.status,
            is_controllable: true,
        }
    }
}

pub struct Fleet {
    devices: Vec<WaterDevice>,
    recent_commands: HashMap<String, i64>,
    random: StdRng,
}

impl Fleet {
    pub fn new(count: usize, now: Instant) -> Self {
        Self::with_seed(count, now, rand::thread_rng().gen())
    }

    fn with_seed(count: usize, now: Instant, seed: u64) -> Self {
        let sectors = ["centro", "campus", "hospital"];
        let mut random = StdRng::seed_from_u64(seed);
        let devices = (0..count)
            .map(|index| WaterDevice {
                id: format!("water_{}_{:02}", sectors[index % 3], index / 3 + 1),
                status: DeviceStatus::StatusOn as i32,
                frequency_secs: 5,
                next_send: now,
                level: random.gen_range(60.0..95.0),
                flow: random.gen_range(30.0..85.0),
                pressure: random.gen_range(2.4..4.6),
                ph: random.gen_range(6.8..7.8),
                turbidity: random.gen_range(0.3..2.0),
                temperature: random.gen_range(17.0..27.0),
                conductivity: random.gen_range(160.0..480.0),
                leak: random.gen_range(0.0..0.25),
            })
            .collect();
        Self {
            devices,
            recent_commands: HashMap::new(),
            random,
        }
    }

    pub fn snapshots(&self) -> Vec<WaterDevice> {
        self.devices.clone()
    }

    pub fn due_snapshots(&mut self, now: Instant) -> Vec<WaterDevice> {
        let mut due = Vec::new();
        for device in &mut self.devices {
            if device.next_send > now {
                continue;
            }
            if device.status == DeviceStatus::StatusOn as i32 {
                // Smooth, bounded values represent a simulated distribution network.
                // The leak estimate lowers pressure and reservoir level together.
                device.leak = (device.leak + self.random.gen_range(-0.08..0.10)).clamp(0.0, 4.0);
                device.level = (device.level + self.random.gen_range(-1.0..1.0)
                    - device.leak * 0.02)
                    .clamp(5.0, 100.0);
                device.flow = (device.flow + self.random.gen_range(-4.0..4.0)).clamp(5.0, 150.0);
                device.pressure = (device.pressure + self.random.gen_range(-0.15..0.15)
                    - device.leak * 0.002)
                    .clamp(0.5, 7.0);
                device.ph = (device.ph + self.random.gen_range(-0.07..0.07)).clamp(6.0, 9.0);
                device.turbidity =
                    (device.turbidity + self.random.gen_range(-0.12..0.15)).clamp(0.0, 10.0);
                device.temperature =
                    (device.temperature + self.random.gen_range(-0.3..0.3)).clamp(10.0, 35.0);
                device.conductivity =
                    (device.conductivity + self.random.gen_range(-8.0..8.0)).clamp(50.0, 1200.0);
            }
            device.next_send = now
                + Duration::from_secs(device.frequency_secs as u64)
                + Duration::from_millis(self.random.gen_range(0..=350));
            due.push(device.clone());
        }
        due
    }

    pub fn apply_command(
        &mut self,
        command: &ConfigCommand,
        timestamp: i64,
        now: Instant,
    ) -> ConfigResponse {
        // Validation, replay protection and mutation occur under the caller's single
        // fleet mutex. Concurrent clients cannot apply the same ID twice.
        let mut rejection = validate_command(command, timestamp).err();
        let index = self
            .devices
            .iter()
            .position(|device| device.id == command.target_device_id);
        if rejection.is_none() && index.is_none() {
            rejection = Some("dispositivo alvo desconhecido".into());
        }
        self.recent_commands.retain(|_, seen| {
            i128::from(timestamp) - i128::from(*seen) <= i128::from(REPLAY_WINDOW_SECS)
        });
        if rejection.is_none() && self.recent_commands.contains_key(&command.command_id) {
            rejection = Some("command_id já processado".into());
        }
        if rejection.is_none() && self.recent_commands.len() >= MAX_RECENT_COMMANDS {
            rejection = Some("limite de comandos recentes atingido; aguarde".into());
        }
        let success = rejection.is_none();
        if let Some(index) = index {
            if success {
                let device = &mut self.devices[index];
                if command.update_status {
                    device.status = command.target_status;
                }
                if command.update_frequency {
                    device.frequency_secs = command.new_frequency_secs;
                }
                device.next_send = now;
                self.recent_commands
                    .insert(command.command_id.clone(), timestamp);
            }
        }
        let device = index.map(|i| &self.devices[i]);
        ConfigResponse {
            message_id: message_id(),
            command_id: command.command_id.clone(),
            timestamp,
            success,
            message: rejection.map_or_else(
                || "Comando aplicado".into(),
                |e| format!("Comando rejeitado: {e}"),
            ),
            updated_status: device.map_or(DeviceStatus::StatusUnknown as i32, |d| d.status),
            updated_frequency_secs: device.map_or(0, |d| d.frequency_secs),
        }
    }

    pub fn mark_all_off(&mut self) {
        for device in &mut self.devices {
            device.status = DeviceStatus::StatusOff as i32;
        }
    }
}

pub fn read_frame(reader: &mut impl Read) -> io::Result<Vec<u8>> {
    let mut header = [0_u8; 4];
    reader.read_exact(&mut header)?;
    let size = u32::from_be_bytes(header) as usize;
    if size == 0 || size > MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "tamanho de frame inválido",
        ));
    }
    let mut payload = vec![0; size];
    reader.read_exact(&mut payload)?;
    Ok(payload)
}

pub fn write_frame(writer: &mut impl Write, message: &impl Message) -> io::Result<()> {
    let payload = message.encode_to_vec();
    if payload.is_empty() || payload.len() > MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "tamanho de resposta inválido",
        ));
    }
    writer.write_all(&(payload.len() as u32).to_be_bytes())?;
    writer.write_all(&payload)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::sync::{Arc, Mutex};
    use std::thread;

    fn command() -> ConfigCommand {
        ConfigCommand {
            command_id: "test-command:1".into(),
            timestamp: 1_800_000_000,
            update_status: true,
            target_status: DeviceStatus::StatusOff as i32,
            update_frequency: true,
            new_frequency_secs: 2,
            target_device_id: "water_centro_01".into(),
        }
    }

    #[test]
    fn default_fleet_has_nine_unique_targetable_devices_and_eight_metrics() {
        let now = Instant::now();
        let mut fleet = Fleet::with_seed(9, now, 17);
        let mut ids: Vec<_> = fleet.snapshots().into_iter().map(|d| d.id).collect();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), 9);
        assert!(ids.iter().all(|id| valid_device_id(id)));
        for device in fleet.due_snapshots(now) {
            assert_eq!(device.metrics().len(), 8);
            assert_eq!(device.discovery("sensor_agua", CONTROL_PORT, 42).r#type, 7);
            assert!(device
                .metrics()
                .iter()
                .all(|metric| metric.value.is_finite()));
        }
        assert!(fleet.due_snapshots(now + Duration::from_secs(1)).is_empty());
    }

    #[test]
    fn off_device_still_announces_status_and_emits_no_physical_metrics() {
        let now = Instant::now();
        let mut fleet = Fleet::with_seed(9, now, 17);
        let response = fleet.apply_command(&command(), command().timestamp, now);
        assert!(response.success);
        let snapshot = fleet.snapshots().remove(0);
        assert!(snapshot.payload(42).metrics.is_empty());
        assert_eq!(
            snapshot
                .discovery("sensor_agua", CONTROL_PORT, 42)
                .initial_status,
            DeviceStatus::StatusOff as i32
        );
        assert_eq!(fleet.snapshots()[1].status, DeviceStatus::StatusOn as i32);
        assert_eq!(fleet.snapshots()[1].frequency_secs, 5);
    }

    #[test]
    fn rejects_malformed_expired_future_invalid_status_and_frequency_commands() {
        let now = command().timestamp;
        let mut bad_commands = Vec::new();
        let mut c = command();
        c.command_id = String::new();
        bad_commands.push(c);
        let mut c = command();
        c.command_id = "id with spaces".into();
        bad_commands.push(c);
        let mut c = command();
        c.target_device_id = "Water_bad".into();
        bad_commands.push(c);
        let mut c = command();
        c.timestamp = now - 301;
        bad_commands.push(c);
        let mut c = command();
        c.timestamp = now + 61;
        bad_commands.push(c);
        let mut c = command();
        c.timestamp = i64::MIN;
        bad_commands.push(c);
        let mut c = command();
        c.timestamp = i64::MAX;
        bad_commands.push(c);
        let mut c = command();
        c.target_status = 3;
        bad_commands.push(c);
        let mut c = command();
        c.new_frequency_secs = 0;
        bad_commands.push(c);
        let mut c = command();
        c.new_frequency_secs = 61;
        bad_commands.push(c);
        let mut c = command();
        c.update_status = false;
        c.update_frequency = false;
        bad_commands.push(c);
        for c in bad_commands {
            assert!(
                validate_command(&c, now).is_err(),
                "accepted invalid command: {c:?}"
            );
        }
        let mut c = command();
        c.timestamp = now - 300;
        assert!(validate_command(&c, now).is_ok());
        c.timestamp = now + 60;
        assert!(validate_command(&c, now).is_ok());
    }

    #[test]
    fn unknown_target_and_invalid_fields_never_partially_mutate_fleet() {
        let now = Instant::now();
        let mut fleet = Fleet::with_seed(9, now, 17);
        let mut c = command();
        c.target_device_id = "water_missing_01".into();
        assert!(!fleet.apply_command(&c, c.timestamp, now).success);
        c.target_device_id = "water_centro_01".into();
        c.new_frequency_secs = 100;
        assert!(!fleet.apply_command(&c, c.timestamp, now).success);
        assert!(fleet
            .snapshots()
            .iter()
            .all(|d| d.status == DeviceStatus::StatusOn as i32 && d.frequency_secs == 5));
    }

    #[test]
    fn concurrent_replayed_command_is_applied_exactly_once() {
        let fleet = Arc::new(Mutex::new(Fleet::with_seed(9, Instant::now(), 17)));
        let workers: Vec<_> = (0..12)
            .map(|_| {
                let fleet = Arc::clone(&fleet);
                thread::spawn(move || {
                    let c = command();
                    fleet
                        .lock()
                        .unwrap()
                        .apply_command(&c, c.timestamp, Instant::now())
                        .success
                })
            })
            .collect();
        assert_eq!(
            workers
                .into_iter()
                .map(|h| h.join().unwrap())
                .filter(|success| *success)
                .count(),
            1
        );
    }

    #[test]
    fn replay_cache_expires_only_after_window() {
        let now = Instant::now();
        let mut fleet = Fleet::with_seed(1, now, 17);
        let mut c = command();
        assert!(fleet.apply_command(&c, c.timestamp, now).success);
        c.timestamp += 600;
        assert!(!fleet.apply_command(&c, c.timestamp, now).success);
        c.timestamp += 1;
        assert!(fleet.apply_command(&c, c.timestamp, now).success);
    }

    #[test]
    fn protobuf_frames_round_trip_and_reject_truncation_and_oversized_headers() {
        let mut bytes = Vec::new();
        write_frame(&mut bytes, &command()).unwrap();
        assert_eq!(
            u32::from_be_bytes(bytes[..4].try_into().unwrap()) as usize,
            bytes.len() - 4
        );
        let decoded =
            ConfigCommand::decode(read_frame(&mut Cursor::new(&bytes)).unwrap().as_slice())
                .unwrap();
        assert_eq!(decoded, command());
        assert!(read_frame(&mut Cursor::new([0, 0, 0, 0])).is_err());
        assert!(read_frame(&mut Cursor::new(u32::MAX.to_be_bytes())).is_err());
        assert!(read_frame(&mut Cursor::new(&bytes[..bytes.len() - 1])).is_err());
    }

    #[test]
    fn measurements_remain_finite_and_within_physical_simulation_ranges() {
        let now = Instant::now();
        let mut fleet = Fleet::with_seed(9, now, 17);
        let bounds = [
            (5.0, 100.0),
            (5.0, 150.0),
            (0.5, 7.0),
            (6.0, 9.0),
            (0.0, 10.0),
            (10.0, 35.0),
            (50.0, 1200.0),
            (0.0, 4.0),
        ];
        for tick in 0..2000 {
            for device in fleet.due_snapshots(now + Duration::from_secs(tick * 6)) {
                for (metric, (low, high)) in device.metrics().iter().zip(bounds) {
                    assert!(
                        metric.value.is_finite() && (low..=high).contains(&metric.value),
                        "{metric:?}"
                    );
                }
            }
        }
    }
}
