"""Transformações de dados do dashboard, independentes da interface Streamlit."""

from __future__ import annotations

import math
from collections.abc import Iterable
from typing import Protocol

import pandas as pd


class HistoryPoint(Protocol):
    timestamp: int
    value: float
    device_id: str


def build_history_frame(points: Iterable[HistoryPoint]) -> pd.DataFrame:
    """Preserva o instante completo e ordena a série, inclusive entre dias."""
    frame = pd.DataFrame(
        ((point.timestamp, point.value, point.device_id) for point in points),
        columns=["timestamp", "value", "device_id"],
    )
    frame["Horário UTC"] = pd.to_datetime(frame["timestamp"], unit="s", utc=True)
    return frame.sort_values("timestamp", kind="stable").reset_index(drop=True)


def build_history_chart(points: Iterable[HistoryPoint]) -> pd.DataFrame:
    """Agrupa rajadas por instante e sensor sem misturar horários de dias distintos."""
    frame = build_history_frame(points)
    return frame.pivot_table(
        index="Horário UTC", columns="device_id", values="value", aggfunc="mean"
    )


def get_metric_reference_range(metric_key: str) -> dict:
    """Faixas de nível usadas pela interface para contextualizar uma média."""
    ranges = {
        "temperature": {
            "safe": (18, 26), "warning": (15, 32),
            "description": "Faixa recomendada: 18-26°C",
        },
        "humidity": {
            "safe": (40, 60), "warning": (30, 70),
            "description": "Faixa recomendada: 40-60%",
        },
        "co2": {
            "safe": (0, 800), "warning": (800, 1200),
            "description": "Nível seguro: < 800 ppm",
        },
        "pm25": {
            "safe": (0, 12), "warning": (12, 35),
            "description": "Limite seguro: ≤ 12 µg/m³",
        },
        "pm10": {
            "safe": (0, 54), "warning": (54, 154),
            "description": "Limite seguro: ≤ 54 µg/m³",
        },
        "luminosity": {
            "safe": (30, 80), "warning": (20, 100),
            "description": "Nível recomendado: 30-80%",
        },
        "power_consumption": {
            "safe": (0, 100), "warning": (100, 200),
            "description": "Consumo normal: 0-100W",
        },
        "queue_length": {
            "safe": (0, 20), "warning": (20, 35),
            "description": "Fila tolerável: até 35 veículos",
        },
    }
    return ranges.get(metric_key, {"description": f"Métrica contínua: {metric_key}"})


def aqi_category(value: float) -> str:
    if value <= 50:
        return "🟢 Bom"
    if value <= 100:
        return "🟡 Moderado"
    if value <= 150:
        return "🟠 Insalubre (sensíveis)"
    if value <= 200:
        return "🔴 Insalubre"
    if value <= 300:
        return "🟣 Muito Insalubre"
    return "⚫ Perigoso"


def assess_analytics_value(
    metric_key: str, value: float, *, is_average: bool
) -> tuple[str, str] | None:
    """Desvio e amplitude não representam níveis físicos para comparar com faixas."""
    if not math.isfinite(value):
        return "warning", "Resultado analítico não finito; não foi possível avaliar a métrica."
    if not is_average:
        return None
    if metric_key == "aqi":
        return "info", f"**Classificação do AQI médio:** {aqi_category(value)}"

    reference = get_metric_reference_range(metric_key)
    if "safe" not in reference or "warning" not in reference:
        return None
    safe_min, safe_max = reference["safe"]
    warn_min, warn_max = reference["warning"]
    description = reference["description"]
    if safe_min <= value <= safe_max:
        return "success", f"**🟢 Média dentro da faixa de referência.**\n\n{description}"
    if warn_min <= value <= warn_max:
        return "warning", f"**🟡 Média fora da faixa ideal.**\n\n{description}"
    return "error", f"**🔴 Média fora da faixa de atenção.**\n\n{description}"
