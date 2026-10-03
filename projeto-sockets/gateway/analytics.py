"""Funções puras usadas pelo pipeline analítico do gateway.

Este módulo não acessa rede, banco de dados, variáveis de ambiente ou Protobuf.
Manter os cálculos isolados torna os rollups reproduzíveis e permite validá-los
com a biblioteca padrão do Python.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable, Sequence, TypeVar


MetricRow = tuple[str, int, str, float, str]
RollupRow = tuple[int, str, str, str, int, float, float, float, float]
RollupSpec = tuple[str, int, int]


@dataclass(frozen=True, slots=True)
class OlapSource:
    """Descrição de uma fonte física para uma consulta OLAP."""

    name: str
    table_name: str
    bucket_size: int
    is_rollup: bool


def validate_time_window(
    start_timestamp: int,
    end_timestamp: int,
    max_window_secs: int | None = None,
) -> int:
    """Valida uma janela inclusiva e retorna sua duração em segundos.

    ``max_window_secs=None`` desativa apenas o limite superior. Timestamps
    negativos não são aceitos porque todas as chamadas do projeto usam Unix
    Epoch; timestamp zero continua válido para que a função permaneça útil em
    testes e cálculos relativos.
    """

    if start_timestamp < 0 or end_timestamp < 0:
        raise ValueError("timestamps não podem ser negativos")
    if start_timestamp > end_timestamp:
        raise ValueError("start_timestamp deve ser menor ou igual a end_timestamp")

    window_secs = end_timestamp - start_timestamp
    if max_window_secs is not None:
        if max_window_secs < 0:
            raise ValueError("max_window_secs não pode ser negativo")
        if window_secs > max_window_secs:
            raise ValueError(
                f"janela de {window_secs}s excede o máximo de {max_window_secs}s"
            )
    return window_secs


def choose_olap_source(
    start_timestamp: int,
    end_timestamp: int,
    raw_max_window_secs: int,
    rollup_1m_max_window_secs: int,
    rollup_5m_max_window_secs: int,
) -> OlapSource:
    """Escolhe deterministicamente a menor granularidade capaz de servir a janela."""

    if not (
        0 <= raw_max_window_secs
        <= rollup_1m_max_window_secs
        <= rollup_5m_max_window_secs
    ):
        raise ValueError("limites OLAP devem ser não negativos e crescentes")

    window_secs = validate_time_window(start_timestamp, end_timestamp)
    if window_secs <= raw_max_window_secs:
        return OlapSource("raw", "metrics", 0, False)
    if window_secs <= rollup_1m_max_window_secs:
        return OlapSource("rollup_1m", "metrics_rollup_1m", 60, True)
    if window_secs <= rollup_5m_max_window_secs:
        return OlapSource("rollup_5m", "metrics_rollup_5m", 300, True)
    return OlapSource("rollup_1h", "metrics_rollup_1h", 3600, True)


def olap_time_range(
    source: OlapSource,
    start_timestamp: int,
    end_timestamp: int,
) -> tuple[int, int]:
    """Alinha os limites da consulta à granularidade da fonte escolhida."""

    validate_time_window(start_timestamp, end_timestamp)
    if not source.is_rollup:
        return start_timestamp, end_timestamp
    if source.bucket_size <= 0:
        raise ValueError("uma fonte de rollup requer bucket_size positivo")

    return (
        (start_timestamp // source.bucket_size) * source.bucket_size,
        (end_timestamp // source.bucket_size) * source.bucket_size,
    )


def choose_retained_olap_source(
    start_timestamp: int,
    end_timestamp: int,
    raw_max_window_secs: int,
    rollup_1m_max_window_secs: int,
    rollup_5m_max_window_secs: int,
    *,
    now_timestamp: int,
    raw_retention_secs: int,
    rollup_1m_retention_secs: int,
    rollup_5m_retention_secs: int,
    rollup_1h_retention_secs: int,
) -> OlapSource:
    """Escolhe a menor granularidade que suporta duração e retenção da janela.

    Retenção zero significa histórico sem expiração. Para rollups, a cobertura
    considera o início do bucket, pois a limpeza do banco usa ``bucket_start``.
    O relógio é recebido explicitamente para manter a escolha determinística.

    Se todas as retenções já expiraram, devolve a fonte de uma hora para que a
    consulta possa retornar normalmente a ausência de dados. A política de
    retenção indica cobertura possível, sem garantir que houve coleta no período.
    """

    first_source = choose_olap_source(
        start_timestamp,
        end_timestamp,
        raw_max_window_secs,
        rollup_1m_max_window_secs,
        rollup_5m_max_window_secs,
    )
    if now_timestamp < 0:
        raise ValueError("now_timestamp não pode ser negativo")

    sources = (
        (OlapSource("raw", "metrics", 0, False), raw_retention_secs),
        (OlapSource("rollup_1m", "metrics_rollup_1m", 60, True), rollup_1m_retention_secs),
        (OlapSource("rollup_5m", "metrics_rollup_5m", 300, True), rollup_5m_retention_secs),
        (OlapSource("rollup_1h", "metrics_rollup_1h", 3600, True), rollup_1h_retention_secs),
    )
    if any(retention_secs < 0 for _, retention_secs in sources):
        raise ValueError("retenções OLAP não podem ser negativas")

    first_index = next(
        index for index, (source, _) in enumerate(sources)
        if source.name == first_source.name
    )
    for source, retention_secs in sources[first_index:]:
        query_start, _ = olap_time_range(source, start_timestamp, end_timestamp)
        if retention_secs == 0 or query_start >= now_timestamp - retention_secs:
            return source

    return sources[-1][0]


def sample_stddev(sample_count: int, value_sum: float, value_sum_sq: float) -> float:
    """Calcula o desvio-padrão amostral a partir de momentos agregados."""

    if sample_count < 0:
        raise ValueError("sample_count não pode ser negativo")
    if not math.isfinite(value_sum) or not math.isfinite(value_sum_sq):
        raise ValueError("momentos agregados devem ser finitos")
    if sample_count <= 1:
        return 0.0

    variance = (value_sum_sq - ((value_sum * value_sum) / sample_count)) / (
        sample_count - 1
    )
    # Erros de arredondamento podem produzir um valor negativo muito pequeno.
    return math.sqrt(max(0.0, variance))


def build_rollup_rows(
    metric_rows: Iterable[MetricRow],
    rollup_specs: Sequence[RollupSpec],
) -> dict[str, list[RollupRow]]:
    """Agrega métricas em todas as granularidades configuradas.

    A saída é ordenada pela chave ``(bucket, device, metric)`` para que testes,
    logs e escritas em lote sejam reproduzíveis entre execuções.
    """

    materialized_rows = list(metric_rows)
    rollup_rows: dict[str, list[RollupRow]] = {}

    for table_name, bucket_size, _retention_secs in rollup_specs:
        if not table_name:
            raise ValueError("nome da tabela de rollup não pode ser vazio")
        if bucket_size <= 0:
            raise ValueError("bucket_size deve ser positivo")

        aggregated: dict[
            tuple[int, str, str], dict[str, float | int | str]
        ] = {}

        for device_id, timestamp, metric_name, value, unit in materialized_rows:
            numeric_value = float(value)
            if not math.isfinite(numeric_value):
                raise ValueError("valores de métricas devem ser finitos")

            bucket_start = (int(timestamp) // bucket_size) * bucket_size
            key = (bucket_start, device_id, metric_name)
            current = aggregated.get(key)

            if current is None:
                aggregated[key] = {
                    "unit": unit,
                    "sample_count": 1,
                    "value_sum": numeric_value,
                    "value_sum_sq": numeric_value * numeric_value,
                    "value_min": numeric_value,
                    "value_max": numeric_value,
                }
            else:
                current["unit"] = unit or current["unit"]
                current["sample_count"] = int(current["sample_count"]) + 1
                current["value_sum"] = float(current["value_sum"]) + numeric_value
                current["value_sum_sq"] = (
                    float(current["value_sum_sq"]) + (numeric_value * numeric_value)
                )
                current["value_min"] = min(
                    float(current["value_min"]), numeric_value
                )
                current["value_max"] = max(
                    float(current["value_max"]), numeric_value
                )

        rollup_rows[table_name] = [
            (
                bucket_start,
                device_id,
                metric_name,
                str(values["unit"]),
                int(values["sample_count"]),
                float(values["value_sum"]),
                float(values["value_sum_sq"]),
                float(values["value_min"]),
                float(values["value_max"]),
            )
            for (bucket_start, device_id, metric_name), values in sorted(
                aggregated.items()
            )
        ]

    return rollup_rows


def graph_sampling_stride(total_points: int, max_points: int) -> int:
    """Retorna o passo mínimo para amostragem uniforme com extremos preservados."""

    if total_points < 0:
        raise ValueError("total_points não pode ser negativo")
    if max_points < 2:
        raise ValueError("max_points deve ser pelo menos 2")
    if total_points <= max_points:
        return 1
    return math.ceil((total_points - 1) / (max_points - 1))


_GraphRowT = TypeVar("_GraphRowT")


def downsample_graph_rows(
    rows: Sequence[_GraphRowT],
    max_points: int,
) -> list[_GraphRowT]:
    """Reduz uma sequência ordenada de modo determinístico.

    A primeira e a última observações sempre são preservadas. O mesmo algoritmo
    de passo é utilizado pela consulta SQL do gateway, evitando diferenças entre
    o comportamento testado e o servido pelo processo.
    """

    stride = graph_sampling_stride(len(rows), max_points)
    if len(rows) <= max_points:
        return list(rows)

    sampled = list(rows[0 : len(rows) - 1 : stride])
    sampled.append(rows[-1])
    return sampled
