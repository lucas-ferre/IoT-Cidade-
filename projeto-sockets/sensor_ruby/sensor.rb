# ====================================================================
# Sensor de Ruído Ambiente (Ruby) — CONTROLÁVEL
#
# Mesmo ciclo de vida dos demais sensores: load-balancer via multicast,
# descoberta (UDP), autenticação (TCP), telemetria (UDP), servidor de controle
# (TCP com AES-GCM + anti-replay da Fase A.2), heartbeat e shutdown gracioso.
# Métricas: noise_db (dB) e peak_db (dB).
# ====================================================================

require 'socket'
require 'ipaddr'
require 'timeout'
require_relative 'messages_pb'
require_relative 'control_crypto'

GATEWAY_HOST = ENV['GATEWAY_HOST'] || 'gateway'
GATEWAY_DISCOVERY_PORT = 5002
AUTH_TCP_PORT = 5007
CONTROL_TCP_PORT = 5011
MULTICAST_GROUP = '239.0.0.1'
MULTICAST_PORT = 5005
SENSOR_LICENSE_PART = ENV['SENSOR_LICENSE_PART'] || 'V1-FULL'
SENSOR_HEX_CODE = '0F'
MAX_TCP_FRAME_BYTES = 1024 * 1024
MANUAL_OVERRIDE_SECS = 30.0
THRESHOLD_EVENT_COOLDOWN_SECS = 3.0
HEARTBEAT_INTERVAL_SECS = [(ENV['SENSOR_HEARTBEAT_INTERVAL_SECS'] || '10').to_f, 1.0].max
HEARTBEAT_JITTER_SECS = [(ENV['SENSOR_HEARTBEAT_JITTER_SECS'] || '2').to_f, 0.0].max
NOISE_DB_THRESHOLD = (ENV['NOISE_DB_THRESHOLD'] || '85').to_f
NOISE_DEVICE_COUNT = [(ENV['NOISE_DEVICE_COUNT'] || '4').to_i, 1].max

SECTORS = [%w[Pici pici], %w[Benfica benfica], %w[Porangabussu porangabussu], %w[Labomar labomar]].freeze

$best_ip = GATEWAY_HOST
$best_score = 999_999.0
$gw_tel_port = 5000
$running = true
$device_ip = '127.0.0.1'
$devices = {}
$mutex = Mutex.new
$replay = ControlCrypto::ReplayGuard.new
$tx = nil

def mono
  Process.clock_gettime(Process::CLOCK_MONOTONIC)
end

def local_ip
  s = UDPSocket.new
  s.connect(GATEWAY_HOST, 5000)
  ip = s.addr.last
  s.close
  ip || '127.0.0.1'
rescue StandardError
  '127.0.0.1'
end

def random_status
  r = rand(100)
  return :STATUS_ON if r < 78
  return :STATUS_OFF if r < 90

  :STATUS_ERROR
end

def build_fleet
  NOISE_DEVICE_COUNT.times do |idx|
    sector_name, slug = SECTORS[idx % SECTORS.length]
    ordinal = (idx / SECTORS.length) + 1
    device_id = format('ruido_%s_%02d', slug, ordinal)
    case slug
    when 'pici' then cx, cy = rand(0..40), rand(0..60)
    when 'benfica' then cx, cy = rand(50..90), rand(0..30)
    when 'porangabussu' then cx, cy = rand(60..100), rand(50..90)
    else cx, cy = rand(0..30), rand(70..100)
    end
    $devices[device_id] = {
      device_id: device_id, sector: sector_name, status: :STATUS_ON,
      frequency_secs: 5, next_send_at: 0.0, manual_until: 0.0,
      last_threshold_send: 0.0, coord_x: cx, coord_y: cy
    }
  end
end

def build_noise_metrics
  noise = 40.0 + (rand * 60.0)          # 40–100 dB (às vezes > 85)
  peak = noise + (rand * 10.0)
  [
    { name: 'noise_db', value: noise, unit: 'dB' },
    { name: 'peak_db', value: peak, unit: 'dB' }
  ]
end

def noise_threshold_reason(metrics)
  m = metrics.find { |x| x[:name] == 'noise_db' }
  return format('noise_db=%.1f >= %.0f', m[:value], NOISE_DB_THRESHOLD) if m && m[:value] >= NOISE_DB_THRESHOLD

  nil
end

def send_udp(data, port)
  $tx.send(data, 0, $best_ip, port)
rescue StandardError => e
  warn "[sensor_ruido] | [UDP:Erro] porta #{port}: #{e.message}"
end

def send_discovery(target_id = nil)
  ids = target_id ? [target_id] : $devices.keys
  ids.each do |id|
    d = $devices[id]
    next unless d

    msg = Smartcity::DiscoveryResponse.new(
      message_id: "DISC-#{id}-#{Time.now.to_i}", timestamp: Time.now.to_i,
      device_id: id, type: :DEVICE_TYPE_NOISE, ip_address: $device_ip,
      control_port: CONTROL_TCP_PORT, initial_status: d[:status],
      is_controllable: true, coord_x: d[:coord_x], coord_y: d[:coord_y]
    )
    send_udp(Smartcity::DiscoveryResponse.encode(msg), GATEWAY_DISCOVERY_PORT)
  end
end

def emit_telemetry(d, metrics, trigger_reason)
  payload = Smartcity::DataPayload.new(
    message_id: "#{d[:device_id]}-#{Time.now.to_i}-#{rand(1000..9999)}",
    timestamp: Time.now.to_i, device_id: d[:device_id], current_status: d[:status],
    metrics: metrics.map { |m| Smartcity::Metric.new(name: m[:name], value: m[:value], unit: m[:unit]) },
    coord_x: d[:coord_x], coord_y: d[:coord_y]
  )
  send_udp(Smartcity::DataPayload.encode(payload), $gw_tel_port)

  if d[:status] == :STATUS_ON && !metrics.empty?
    nb = metrics.find { |m| m[:name] == 'noise_db' }
    label = trigger_reason ? 'Evento por limiar' : 'Telemetria injetada'
    puts "[sensor_ruido] | [UDP] #{label} | Dispositivo=#{d[:device_id]} | Setor=#{d[:sector]} | " \
         "Status=#{d[:status]} | Ruido=#{format('%.1f', nb ? nb[:value] : 0)}dB" \
         "#{trigger_reason ? " | Limiar=#{trigger_reason}" : ''}"
  elsif d[:status] != :STATUS_ON
    puts "[sensor_ruido] | [UDP] Heartbeat | Dispositivo=#{d[:device_id]} | Status=#{d[:status]}"
  end
end

def authenticate
  Timeout.timeout(12) do
    sock = TCPSocket.new(GATEWAY_HOST, AUTH_TCP_PORT)
    req = Smartcity::AuthRequest.new(
      device_id: $devices.keys.first, type: :DEVICE_TYPE_NOISE,
      license_key_part: SENSOR_LICENSE_PART, hex_service_code: SENSOR_HEX_CODE
    )
    body = Smartcity::AuthRequest.encode(req)
    sock.write([body.bytesize].pack('N') + body)

    header = sock.read(4)
    raise 'cabeçalho ausente' unless header && header.bytesize == 4

    len = header.unpack1('N')
    resp = Smartcity::AuthResponse.decode(sock.read(len))
    sock.close
    unless resp.success
      warn "[sensor_ruido] | [Auth] FALHA: #{resp.message}"
      exit(1)
    end
    puts "[Auth] Gateway encontrado! Tipo: sensor_ruido, Chave: '#{SENSOR_LICENSE_PART}-#{SENSOR_HEX_CODE}'. " \
         "Validação: SUCESSO. Porta alocada e conectada: #{resp.assigned_port}."
    resp.assigned_port
  end
rescue StandardError => e
  warn "[sensor_ruido] | [Auth] Erro: #{e.message}"
  exit(1)
end

def handle_control_client(client)
  header = client.read(4)
  return client.close unless header && header.bytesize == 4

  len = header.unpack1('N')
  return client.close if len <= 0 || len > MAX_TCP_FRAME_BYTES

  body = client.read(len)
  body = ControlCrypto.unwrap(body) if ControlCrypto::SECURE
  cmd = Smartcity::ConfigCommand.decode(body)

  if ControlCrypto::SECURE
    ok, reason = $replay.check(cmd.command_id, cmd.timestamp)
    unless ok
      warn "[sensor_ruido] | [TCP] Comando rejeitado (anti-replay): #{reason}"
      send_response(client, Smartcity::ConfigResponse.new(
        command_id: cmd.command_id, success: false,
        message: "Comando rejeitado (anti-replay): #{reason}"
      ))
      return
    end
  end

  target_id = (cmd.target_device_id && !cmd.target_device_id.empty?) ? cmd.target_device_id : $devices.keys.first
  d = $devices[target_id]
  unless d
    send_response(client, Smartcity::ConfigResponse.new(
      command_id: cmd.command_id, success: false, message: "Dispositivo alvo desconhecido: #{target_id}"
    ))
    return
  end

  $mutex.synchronize do
    if cmd.update_status
      d[:status] = cmd.target_status
      d[:manual_until] = mono + MANUAL_OVERRIDE_SECS
    end
    d[:frequency_secs] = cmd.new_frequency_secs if cmd.update_frequency && cmd.new_frequency_secs > 0
    d[:next_send_at] = 0.0
  end

  puts "[sensor_ruido] | [TCP] Comando #{cmd.command_id} | Dispositivo=#{target_id} | " \
       "Status=#{d[:status]} | Frequencia=#{d[:frequency_secs]}s"
  send_response(client, Smartcity::ConfigResponse.new(
    message_id: "ACK-#{Time.now.to_i}", command_id: cmd.command_id, timestamp: Time.now.to_i,
    success: true, message: "Sensor de ruido #{target_id} reconfigurado com sucesso.",
    updated_status: d[:status], updated_frequency_secs: d[:frequency_secs]
  ))
  send_discovery(target_id)
rescue StandardError => e
  warn "[sensor_ruido] | [TCP:Erro] #{e.message}"
  begin
    send_response(client, Smartcity::ConfigResponse.new(success: false, message: "Falha: #{e.message}"))
  rescue StandardError
    nil
  end
ensure
  begin
    client.close unless client.closed?
  rescue StandardError
    nil
  end
end

def send_response(client, resp)
  out = Smartcity::ConfigResponse.encode(resp)
  out = ControlCrypto.wrap(out) if ControlCrypto::SECURE
  client.write([out.bytesize].pack('N') + out)
end

def start_control_server
  Thread.new do
    server = TCPServer.new('0.0.0.0', CONTROL_TCP_PORT)
    puts "[sensor_ruido] | [TCP] Interface de controle ativa na porta #{CONTROL_TCP_PORT}."
    while $running
      begin
        client = server.accept
        Thread.new(client) { |c| handle_control_client(c) }
      rescue StandardError => e
        warn "[sensor_ruido] | [TCP:Erro] accept: #{e.message}" if $running
      end
    end
  end
end

def start_multicast_listener
  Thread.new do
    sock = UDPSocket.new
    sock.setsockopt(Socket::SOL_SOCKET, Socket::SO_REUSEADDR, 1)
    sock.bind('0.0.0.0', MULTICAST_PORT)
    mreq = IPAddr.new(MULTICAST_GROUP).hton + IPAddr.new('0.0.0.0').hton
    sock.setsockopt(Socket::IPPROTO_IP, Socket::IP_ADD_MEMBERSHIP, mreq)
    puts "[sensor_ruido] | [Multicast] Escutando #{MULTICAST_GROUP}:#{MULTICAST_PORT}."
    while $running
      begin
        data, = sock.recvfrom(2048)
        load = Smartcity::AggregatorLoad.decode(data)
        if load.aggregator_id == 'GATEWAY_PROBE'
          Thread.new { sleep(rand * 2.0); send_discovery if $running }
        elsif !load.ip_address.empty?
          score = (load.cpu_load * 0.4) + (load.queue_size * 0.6)
          $mutex.synchronize do
            if score < $best_score || $best_ip == load.ip_address
              if $best_ip != load.ip_address
                puts "[sensor_ruido] | [LoadBalancer] Rota -> #{load.aggregator_id} " \
                     "(Score: #{format('%.2f', $best_score)} -> #{format('%.2f', score)})"
                $best_ip = load.ip_address
              end
              $best_score = score
            end
          end
        end
      rescue StandardError
        next
      end
    end
  end
end

def start_heartbeat
  Thread.new do
    while $running
      sleep(HEARTBEAT_INTERVAL_SECS + (rand * HEARTBEAT_JITTER_SECS))
      break unless $running

      puts '[sensor_ruido] | [Heartbeat] Renovando presença da frota via DiscoveryResponse.'
      send_discovery
    end
  end
end

def telemetry_loop
  while $running
    now = mono
    $devices.each_value do |d|
      $mutex.synchronize do
        if d[:status] == :STATUS_ON && (now - d[:last_threshold_send]) >= THRESHOLD_EVENT_COOLDOWN_SECS
          metrics = build_noise_metrics
          reason = noise_threshold_reason(metrics)
          if reason
            d[:last_threshold_send] = now
            emit_telemetry(d, metrics, reason)
          end
        end

        if now >= d[:next_send_at]
          d[:status] = random_status if now >= d[:manual_until]
          d[:next_send_at] = now + d[:frequency_secs] + (rand * 0.35)
          metrics = d[:status] == :STATUS_ON ? build_noise_metrics : []
          emit_telemetry(d, metrics, nil)
        end
      end
    end
    sleep(0.2)
  end
end

def main
  $device_ip = local_ip
  build_fleet

  puts '============================================================'
  puts "[sensor_ruido] | Inicializando frota de #{$devices.size} sensor(es) de ruido."
  $devices.each_value { |d| puts "[sensor_ruido] | Dispositivo=#{d[:device_id]} | Setor=#{d[:sector]}" }
  puts '[sensor_ruido] | Metricas: noise_db (dB), peak_db (dB)'
  puts '============================================================'

  $tx = UDPSocket.new
  start_multicast_listener

  puts '[sensor_ruido] | Aguardando broadcast de AggregatorLoad para descobrir IP real...'
  sleep(0.1) while $best_ip == GATEWAY_HOST && $running
  return unless $running

  send_discovery
  $gw_tel_port = authenticate
  sleep(2.0)

  start_control_server
  start_heartbeat

  if %w[1 true yes on].include?((ENV['SENSOR_IDLE_CONSOLE'] || '').downcase)
    begin
      require_relative 'console'
      Thread.new { Console.run('127.0.0.1', CONTROL_TCP_PORT) }
      puts "[sensor_ruido] | [IDLE] Console embutido ativo (use 'docker attach')."
    rescue StandardError => e
      warn "[sensor_ruido] | [IDLE] Falha ao iniciar console: #{e.message}"
    end
  end

  telemetry_loop
end

%w[TERM INT].each do |sig|
  Signal.trap(sig) do
    $running = false
  end
end

main
