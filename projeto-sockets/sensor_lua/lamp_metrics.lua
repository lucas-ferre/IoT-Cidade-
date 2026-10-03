local LampMetrics = {}

-- Integra a última potência medida, apenas durante períodos ligados.
-- O acumulador representa kWh desde o início do processo, por dispositivo.
function LampMetrics.update_energy(device, now)
    local elapsed = math.max(0, now - (device.energy_updated_at or now))
    if device.energy_status == "STATUS_ON" then
        device.energy_kwh = (device.energy_kwh or 0)
            + (device.power_w or 30.0) * elapsed / 3600000.0
    end
    device.energy_updated_at = now
    device.energy_status = device.status
end

function LampMetrics.sample(device, now)
    LampMetrics.update_energy(device, now)
    local ambient_light = 5.0 + math.random() * 895.0
    local dimming_level = math.max(75.0, 100.0 - ambient_light / 35.0)
    local luminosity = math.floor(dimming_level + 0.5)
    local power = 5.0 + dimming_level * 0.30
    local voltage = 218.0 + math.random() * 14.0
    -- Potência ativa = tensão * corrente * fator de potência (0,95).
    local current = power / (voltage * 0.95)
    local led_temperature = 28.0 + power * 0.70 + math.random() * 3.0
    device.power_w = power
    return {
        { name = "luminosity", value = luminosity, unit = "%" },
        { name = "power_consumption", value = power, unit = "W" },
        { name = "energy_consumption", value = device.energy_kwh or 0.0, unit = "kWh" },
        { name = "voltage", value = voltage, unit = "V" },
        { name = "current", value = current, unit = "A" },
        { name = "led_temperature", value = led_temperature, unit = "C" },
        { name = "ambient_light", value = ambient_light, unit = "lux" },
        { name = "dimming_level", value = dimming_level, unit = "%" }
    }
end

return LampMetrics
