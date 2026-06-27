-- ====================================================================
-- Console de controle interativo do Poste Inteligente (Lua)
--
-- Modos de uso:
--
-- 1. STANDALONE (processo separado) — conecta na porta de controle TCP do
--    sensor e envia ConfigCommand (mesmo contrato do Gateway/Dashboard):
--
--        docker exec -it sensor_posto sh -c \
--          'eval $(luarocks --lua-version=5.4 path) && lua5.4 console.lua'
--        # ou: lua5.4 console.lua <host> <porta>
--
-- 2. EMBUTIDO (IDLE no próprio processo do sensor) — sensor.lua chama
--    console.init(ctx) + console.poll() no loop principal quando
--    SENSOR_IDLE_CONSOLE está ligado. Como o sensor é mono-thread, o poll de
--    stdin é NÃO-BLOQUEANTE (via luaposix); se luaposix faltar, o modo
--    embutido é desativado graciosamente (o standalone continua funcionando).
--    Requer terminal anexado (docker compose: stdin_open + tty):
--
--        docker attach sensor_posto         # Ctrl-P Ctrl-Q para desanexar
--
-- Protocolo (standalone): prefixo >I4 + Protobuf ConfigCommand, uma conexão
-- por comando. Comandos: status, on, off, err, freq, help, quit.
-- ====================================================================

local socket = require("socket")
local pb     = require("pb")
local control_crypto = require("control_crypto")

local M = {}

local DEFAULT_HOST = "127.0.0.1"
local DEFAULT_PORT = 5006 -- CONTROL_TCP_PORT do poste
local MAX_FRAME_BYTES = 1024 * 1024

local HELP_TEXT = [[
Comandos do console (poste Lua):
  status [device_id]            Lê o estado atual.
  on     [device_id]            Liga o dispositivo (STATUS_ON).
  off    [device_id]            Desliga o dispositivo (STATUS_OFF).
  err    [device_id]            Marca falha (STATUS_ERROR).
  freq <segundos> [device_id]   Altera o intervalo de telemetria.
  help                          Mostra esta ajuda.
  quit / exit                   Sai do console.

Sem device_id, aplica ao dispositivo padrão (primeiro da frota).]]

local STATUS_BY_CMD = {
    on  = "STATUS_ON",
    off = "STATUS_OFF",
    err = "STATUS_ERROR",
}

-- --------------------------------------------------------------------
-- Util de parsing de linha → { cmd, args = {...} }
-- --------------------------------------------------------------------
local function parse_line(line)
    local parts = {}
    for token in string.gmatch(line, "%S+") do
        parts[#parts + 1] = token
    end
    return parts
end

-- ====================================================================
-- MODO STANDALONE (cliente TCP)
-- ====================================================================

local function recv_exact(client, n)
    local chunks, received = {}, 0
    while received < n do
        local chunk, err, partial = client:receive(n - received)
        if chunk then
            chunks[#chunks + 1] = chunk
            received = received + #chunk
        elseif partial and #partial > 0 then
            chunks[#chunks + 1] = partial
            received = received + #partial
            if err ~= "timeout" then break end
        else
            return nil, err or "conexão encerrada"
        end
    end
    if received < n then return nil, "frame incompleto" end
    return table.concat(chunks)
end

local function send_command_tcp(host, port, fields)
    local msg = {
        command_id         = string.format("CONSOLE-%04d", math.random(1, 9999)),
        timestamp          = os.time(),
        update_status      = fields.update_status or false,
        target_status      = fields.target_status or "STATUS_ON",
        update_frequency   = fields.update_frequency or false,
        new_frequency_secs = fields.new_frequency_secs or 0,
        target_device_id   = fields.target_device_id or "",
    }
    local payload = assert(pb.encode("smartcity.ConfigCommand", msg))
    if control_crypto.SECURE then
        payload = control_crypto.wrap(payload)
    end

    local client = socket.tcp()
    client:settimeout(5.0)

    local ip = host
    if not host:match("^%d+%.%d+%.%d+%.%d+$") then
        local resolved = socket.dns.toip(host)
        ip = resolved or host
    end

    local ok, err = client:connect(ip, port)
    if not ok then
        client:close()
        return nil, "conexão falhou: " .. tostring(err)
    end

    client:send(string.pack(">I4", #payload) .. payload)

    local header, herr = recv_exact(client, 4)
    if not header then
        client:close()
        return nil, "cabeçalho: " .. tostring(herr)
    end
    local len = string.unpack(">I4", header)
    if len <= 0 or len > MAX_FRAME_BYTES then
        client:close()
        return nil, "frame de resposta inválido: " .. tostring(len)
    end
    local body, berr = recv_exact(client, len)
    client:close()
    if not body then return nil, "payload: " .. tostring(berr) end

    if control_crypto.SECURE then
        local ok_unwrap, plain = pcall(control_crypto.unwrap, body)
        if not ok_unwrap then return nil, "falha ao decifrar resposta: " .. tostring(plain) end
        body = plain
    end

    local ok_dec, resp = pcall(pb.decode, "smartcity.ConfigResponse", body)
    if not ok_dec then return nil, "decode falhou" end
    return resp
end

local function print_response(resp)
    local ok = resp.success and "✓" or "✗"
    print(string.format("  %s %s", ok, (resp.message ~= "" and resp.message) or "(sem mensagem)"))
    print(string.format("    status=%s | frequência=%ss | cmd=%s",
        tostring(resp.updated_status), tostring(resp.updated_frequency_secs), tostring(resp.command_id)))
end

local function dispatch_tcp(host, port, line)
    local parts = parse_line(line)
    if #parts == 0 then return true end
    local cmd = string.lower(parts[1])

    if cmd == "quit" or cmd == "exit" then return false end
    if cmd == "help" or cmd == "?" then print(HELP_TEXT); return true end

    local fields, dev_index
    if cmd == "status" then
        fields, dev_index = {}, 2
    elseif STATUS_BY_CMD[cmd] then
        fields, dev_index = { update_status = true, target_status = STATUS_BY_CMD[cmd] }, 2
    elseif cmd == "freq" then
        local secs = tonumber(parts[2])
        if not secs or secs <= 0 then
            print("  Uso: freq <segundos> [device_id]  (segundos > 0)")
            return true
        end
        fields, dev_index = { update_frequency = true, new_frequency_secs = math.floor(secs) }, 3
    else
        print(string.format("  Comando desconhecido: '%s'. Digite 'help'.", cmd))
        return true
    end

    fields.target_device_id = parts[dev_index] or ""
    local resp, err = send_command_tcp(host, port, fields)
    if resp then
        print_response(resp)
    else
        print(string.format("  ✗ Falha de comunicação com %s:%d: %s", host, port, tostring(err)))
    end
    return true
end

function M.run_standalone(host, port)
    host = host or DEFAULT_HOST
    port = port or DEFAULT_PORT
    -- O descritor Protobuf é o mesmo carregado pelo sensor (gerado no build).
    pcall(pb.loadfile, "messages.pb")
    math.randomseed(os.time())

    print("============================================================")
    print(string.format("[Console Lua] Interface de controle do poste (%s:%d).", host, port))
    print("[Console Lua] Digite 'help' para ver os comandos, 'quit' para sair.")
    print("============================================================")

    while true do
        io.write("poste> ")
        io.flush()
        local line = io.read("l")
        if line == nil then
            print("\n[Console Lua] EOF — encerrando console.")
            return
        end
        if not dispatch_tcp(host, port, line) then
            print("[Console Lua] Console encerrado.")
            return
        end
    end
end

-- ====================================================================
-- MODO EMBUTIDO (in-memory, poll não-bloqueante de stdin)
-- ====================================================================

local embedded = {
    enabled   = false,
    ctx       = nil,
    poll_mod  = nil,
}

-- ctx = { devices, device_order, default_device_id, manual_override_secs }
function M.init(ctx)
    embedded.ctx = ctx
    local ok_poll, P = pcall(require, "posix.poll")
    if ok_poll and P and P.poll then
        embedded.poll_mod = P
        embedded.enabled = true
        print("[Console Lua:IDLE] Console embutido ativo (poll de stdin via luaposix).")
        print("[Console Lua:IDLE] Use 'docker attach'; digite 'help'.")
    else
        embedded.enabled = false
        print("[Console Lua:IDLE] luaposix/poll indisponível — console embutido desativado.")
        print("[Console Lua:IDLE] Use o modo standalone: lua5.4 console.lua")
    end
    return embedded.enabled
end

local function embedded_apply(line)
    local ctx = embedded.ctx
    local parts = parse_line(line)
    if #parts == 0 then return end
    local cmd = string.lower(parts[1])

    if cmd == "help" or cmd == "?" then print(HELP_TEXT); return end
    if cmd == "quit" or cmd == "exit" then
        print("[Console Lua:IDLE] (no modo embutido 'quit' não encerra o sensor; use docker stop)")
        return
    end

    local function resolve(idx)
        local id = parts[idx]
        if not id or id == "" then return ctx.default_device_id end
        return id
    end

    if cmd == "status" then
        local d = ctx.devices[resolve(2)]
        if d then
            print(string.format("  ✓ %s | setor=%s | status=%s | freq=%ss",
                d.device_id, d.sector, d.status, tostring(d.frequency_secs)))
        else
            print("  ✗ dispositivo desconhecido.")
        end
    elseif STATUS_BY_CMD[cmd] then
        local d = ctx.devices[resolve(2)]
        if d then
            d.status = STATUS_BY_CMD[cmd]
            d.manual_until = ctx.socket.gettime() + ctx.manual_override_secs
            d.last_udp_send = 0
            print(string.format("  ✓ %s → status=%s", d.device_id, d.status))
        else
            print("  ✗ dispositivo desconhecido.")
        end
    elseif cmd == "freq" then
        local secs = tonumber(parts[2])
        if not secs or secs <= 0 then
            print("  Uso: freq <segundos> [device_id]  (segundos > 0)")
            return
        end
        local d = ctx.devices[resolve(3)]
        if d then
            d.frequency_secs = math.floor(secs)
            d.last_udp_send = 0
            print(string.format("  ✓ %s → frequência=%ss", d.device_id, d.frequency_secs))
        else
            print("  ✗ dispositivo desconhecido.")
        end
    else
        print(string.format("  Comando desconhecido: '%s'. Digite 'help'.", cmd))
    end
end

-- Chamado a cada iteração do loop principal do sensor. Não bloqueia.
function M.poll()
    if not embedded.enabled then return end
    local fds = { [0] = { events = { IN = true } } }
    local ok, _ = pcall(embedded.poll_mod.poll, fds, 0)
    if not ok then
        embedded.enabled = false
        return
    end
    if fds[0].revents and fds[0].revents.IN then
        local line = io.read("l")
        if line == nil then
            embedded.enabled = false  -- EOF no stdin
            return
        end
        local ok_apply, err = pcall(embedded_apply, line)
        if not ok_apply then
            print("[Console Lua:IDLE] erro ao processar comando: " .. tostring(err))
        end
    end
end

-- ====================================================================
-- Execução direta → modo standalone
-- ====================================================================
if arg and arg[0] and arg[0]:match("console%.lua$") then
    M.run_standalone(arg[1], arg[2] and tonumber(arg[2]) or nil)
end

return M
