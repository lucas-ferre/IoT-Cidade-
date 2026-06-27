# ====================================================================
# Console de controle do Sensor de Ruído (Ruby)
#
# Modos:
#   STANDALONE:  docker exec -it sensor_ruido ruby console.rb [host] [porta]
#   EMBUTIDO:    SENSOR_IDLE_CONSOLE=1 + docker attach sensor_ruido
#
# Protocolo length-prefix (>I4/'N') + Protobuf ConfigCommand, com AES-GCM se
# CONTROL_SECURE=1. Uma conexão por comando.
# Comandos: status, on, off, err, freq <s>, help, quit.
# ====================================================================

require 'socket'
require 'timeout'
require_relative 'messages_pb'
require_relative 'control_crypto'

module Console
  DEFAULT_HOST = '127.0.0.1'
  DEFAULT_PORT = 5011
  MAX_FRAME = 1024 * 1024

  HELP = <<~TXT
    Comandos do console (sensor de ruido):
      status [device_id]            Lê o estado atual.
      on     [device_id]            Liga (STATUS_ON).
      off    [device_id]            Desliga (STATUS_OFF).
      err    [device_id]            Marca falha (STATUS_ERROR).
      freq <segundos> [device_id]   Altera o intervalo de telemetria.
      help                          Mostra esta ajuda.
      quit / exit                   Sai do console.
  TXT

  def self.send_command(host, port, fields)
    cmd = Smartcity::ConfigCommand.new(
      command_id: "CONSOLE-#{rand(1_000_000)}", timestamp: Time.now.to_i,
      update_status: fields[:update_status] || false,
      target_status: fields[:target_status] || :STATUS_ON,
      update_frequency: fields[:update_frequency] || false,
      new_frequency_secs: fields[:new_frequency_secs] || 0,
      target_device_id: fields[:target_device_id] || ''
    )
    body = Smartcity::ConfigCommand.encode(cmd)
    body = ControlCrypto.wrap(body) if ControlCrypto::SECURE

    Timeout.timeout(5) do
      sock = TCPSocket.new(host, port)
      sock.write([body.bytesize].pack('N') + body)
      header = sock.read(4)
      raise 'sem resposta do sensor' unless header && header.bytesize == 4

      len = header.unpack1('N')
      raise "frame inválido: #{len}" if len <= 0 || len > MAX_FRAME

      resp_body = sock.read(len)
      sock.close
      resp_body = ControlCrypto.unwrap(resp_body) if ControlCrypto::SECURE
      Smartcity::ConfigResponse.decode(resp_body)
    end
  end

  def self.dispatch(host, port, line)
    parts = line.strip.split(/\s+/)
    return true if parts.empty?

    cmd = parts[0].downcase
    return false if %w[quit exit].include?(cmd)
    if %w[help ?].include?(cmd)
      puts HELP
      return true
    end

    fields = nil
    dev_idx = 1
    case cmd
    when 'status' then fields = {}
    when 'on'  then fields = { update_status: true, target_status: :STATUS_ON }
    when 'off' then fields = { update_status: true, target_status: :STATUS_OFF }
    when 'err' then fields = { update_status: true, target_status: :STATUS_ERROR }
    when 'freq'
      secs = parts[1].to_i
      if secs <= 0
        puts '  Uso: freq <segundos> [device_id]'
        return true
      end
      fields = { update_frequency: true, new_frequency_secs: secs }
      dev_idx = 2
    else
      puts "  Comando desconhecido: '#{cmd}'. Digite 'help'."
      return true
    end

    fields[:target_device_id] = parts[dev_idx] || ''
    begin
      resp = send_command(host, port, fields)
      puts "  #{resp.success ? '✓' : '✗'} #{resp.message}"
      puts "    status=#{resp.updated_status} | frequência=#{resp.updated_frequency_secs}s | cmd=#{resp.command_id}"
    rescue StandardError => e
      puts "  ✗ Falha de comunicação com #{host}:#{port}: #{e.message}"
    end
    true
  end

  def self.run(host = DEFAULT_HOST, port = DEFAULT_PORT)
    puts '============================================================'
    puts "[Console Ruby] Controle do sensor de ruido (#{host}:#{port})."
    puts "[Console Ruby] Digite 'help' para os comandos, 'quit' para sair."
    puts '============================================================'
    loop do
      print 'ruido> '
      $stdout.flush
      line = $stdin.gets
      break if line.nil?
      break unless dispatch(host, port, line)
    end
    puts '[Console Ruby] Console encerrado.'
  end
end

if __FILE__ == $PROGRAM_NAME
  host = ARGV[0] || Console::DEFAULT_HOST
  port = (ARGV[1] || Console::DEFAULT_PORT).to_i
  Console.run(host, port)
end
