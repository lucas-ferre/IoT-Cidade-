local LampMetrics = require("lamp_metrics")
math.randomseed(42)
local device = { status = "STATUS_ON", energy_status = "STATUS_ON",
                 energy_updated_at = 0, power_w = 30, energy_kwh = 0 }

LampMetrics.update_energy(device, 3600)
assert(math.abs(device.energy_kwh - 0.03) < 1e-10)
device.status = "STATUS_OFF"
LampMetrics.update_energy(device, 3600)
LampMetrics.update_energy(device, 7200)
assert(math.abs(device.energy_kwh - 0.03) < 1e-10, "desligado não consome energia")
device.status = "STATUS_ON"
LampMetrics.update_energy(device, 7200)

local previous_energy = device.energy_kwh
for sample = 1, 1000 do
    local metrics = LampMetrics.sample(device, 7200 + sample * 5)
    assert(#metrics == 8)
    local values = {}
    for _, metric in ipairs(metrics) do
        assert(values[metric.name] == nil and metric.unit ~= "")
        assert(metric.value == metric.value and metric.value >= 0)
        values[metric.name] = metric.value
    end
    assert(values.luminosity >= 75 and values.luminosity <= 100)
    assert(values.dimming_level >= 75 and values.dimming_level <= 100)
    assert(values.voltage >= 218 and values.voltage <= 232)
    assert(math.abs(values.power_consumption - values.voltage * values.current * 0.95) < 1e-10)
    assert(values.energy_consumption >= previous_energy)
    previous_energy = values.energy_consumption
end
print("Postes: 1.000 amostras, 8 métricas, energia e potência validadas.")
