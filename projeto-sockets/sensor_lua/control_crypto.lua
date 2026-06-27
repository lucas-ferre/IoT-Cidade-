-- ====================================================================
-- Criptografia + anti-replay do canal de controle (poste Lua)
--
-- Esquema idêntico ao do gateway/Python e do semáforo/Java:
--   Frame cifrado = nonce[12] || ciphertext || tag[16]   (AES-128-GCM)
--   Chave         = AES_SECRET_KEY (UTF-8, padding/truncate p/ 16 bytes)
--   Anti-replay   = timestamp (janela de skew) + cache de command_id.
--
-- Ligado por CONTROL_SECURE=1. Usa luaossl (openssl.cipher/openssl.rand),
-- carregado preguiçosamente — o módulo importa mesmo sem a lib quando o modo
-- seguro está desligado. Compartilhado por sensor.lua e console.lua.
-- ====================================================================

local M = {}

local function get_secret(name, default)
    local file_path = os.getenv(name .. "_FILE")
    if file_path then
        local fh = io.open(file_path, "r")
        if fh then
            local content = fh:read("*a")
            fh:close()
            if content then return (content:gsub("%s+$", "")) end
        end
    end
    return os.getenv(name) or default
end

local raw_key = get_secret("AES_SECRET_KEY", "SmartCityKey1234")
-- padding com \0 e truncate p/ 16 bytes (igual ao ljust(16,0)[:16] do Python)
local KEY = (raw_key .. string.rep("\0", 16)):sub(1, 16)

local function env_truthy(name)
    local v = string.lower(os.getenv(name) or "")
    return v == "1" or v == "true" or v == "yes" or v == "on"
end

M.SECURE = env_truthy("CONTROL_SECURE")
M.MAX_SKEW_SECS = math.max(1, tonumber(os.getenv("CONTROL_MAX_SKEW_SECS") or "30") or 30)

local NONCE_LEN = 12
local TAG_LEN = 16

local _cipher, _rand
local function load_crypto()
    if not _cipher then
        _cipher = require("openssl.cipher")
        _rand = require("openssl.rand")
    end
end

function M.wrap(plaintext)
    load_crypto()
    local iv = _rand.bytes(NONCE_LEN)
    local c = _cipher.new("aes-128-gcm"):encrypt(KEY, iv)
    local ct = (c:update(plaintext) or "") .. (c:final() or "")
    local tag = c:getTag(TAG_LEN)
    return iv .. ct .. tag
end

function M.unwrap(blob)
    load_crypto()
    if #blob < NONCE_LEN + TAG_LEN then
        error("frame cifrado muito curto")
    end
    local iv  = blob:sub(1, NONCE_LEN)
    local tag = blob:sub(#blob - TAG_LEN + 1)
    local ct  = blob:sub(NONCE_LEN + 1, #blob - TAG_LEN)
    local d = _cipher.new("aes-128-gcm"):decrypt(KEY, iv)
    d:setTag(tag)                       -- tag deve ser setada antes do final()
    return (d:update(ct) or "") .. (d:final() or "")
end

-- ---- Anti-replay ----------------------------------------------------
local Guard = {}
Guard.__index = Guard

function M.new_replay_guard(max_skew)
    return setmetatable({ max_skew = max_skew or M.MAX_SKEW_SECS, seen = {} }, Guard)
end

function Guard:check(command_id, ts)
    local now = os.time()
    if ts and ts ~= 0 and math.abs(now - ts) > self.max_skew then
        return false, string.format("timestamp fora da janela (±%ds)", self.max_skew)
    end
    -- Poda entradas expiradas (mantém a tabela pequena).
    for id, seen_ts in pairs(self.seen) do
        if now - seen_ts > self.max_skew then self.seen[id] = nil end
    end
    if command_id and command_id ~= "" then
        if self.seen[command_id] then
            return false, "command_id repetido (replay)"
        end
        self.seen[command_id] = now
    end
    return true, nil
end

return M
