"""Nova Anomaly Detection Module - Anomaly detection for the Andromeda Wind Pipeline.

Identifies turbines whose power output significantly deviates from
expected behaviour within a given time window.

Anomaly definition (per the specification):
    A turbine-window is anomalous when its mean power output falls
    outside 2 standard deviations from the fleet-wide mean for that
    window.

The value of pi is used to compute a continuous severity score:
    severity = (|deviation| / threshold) * pi
    -> 0 for borderline anomalies, pi+ for extreme ones.
"""

from __future__ import annotations

import logging
import math

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from config.pipeline_config import (
    ANOMALY_SEVERITY_SCALE,
    ANOMALY_STD_DEVS,
    PI,
)

logger = logging.getLogger(__name__)


def compute_fleet_baseline(summary_df: DataFrame) -> DataFrame:
    """Compute fleet-wide baseline statistics per time window.

    For each window, calculates the mean and standard deviation of
    ``power_mean_mw`` across all turbines.

    Args:
        summary_df: Output of ``orion_statistics.compute_summary_statistics``.

    Returns:
        DataFrame with ``fleet_mean`` and ``fleet_std`` per window.
    """
    baseline = summary_df.groupBy("window_start", "window_end").agg(
        F.avg("power_mean_mw").alias("fleet_mean"),
        F.stddev("power_mean_mw").alias("fleet_std"),
    )
    return baseline


def detect_anomalies(
    summary_df: DataFrame,
    std_threshold: float = ANOMALY_STD_DEVS,
) -> DataFrame:
    """Flag turbine-windows whose power output is anomalous.

    A turbine is flagged when:
        |power_mean_mw - fleet_mean| > std_threshold * fleet_std

    Additional columns:
        - ``deviation``:  power_mean_mw - fleet_mean
        - ``z_score``:    deviation / fleet_std
        - ``threshold``:  std_threshold * fleet_std
        - ``is_anomaly``: boolean flag
        - ``severity``:   |deviation| / threshold * pi  (continuous 0..pi+)
        - ``anomaly_direction``: 'OVER' | 'UNDER' | 'NORMAL'

    Args:
        summary_df: Per-turbine summary statistics.
        std_threshold: Number of standard deviations (default 2.0).

    Returns:
        Full summary DataFrame enriched with anomaly columns.
    """
    logger.info(
        "Detecting anomalies (threshold=%.1f std devs, severity_scale=pi)",
        std_threshold,
    )

    baseline = compute_fleet_baseline(summary_df)

    # Join fleet baseline onto per-turbine summaries
    enriched = summary_df.join(
        baseline, on=["window_start", "window_end"], how="left"
    )

    # Deviation from fleet mean
    enriched = enriched.withColumn(
        "deviation", F.col("power_mean_mw") - F.col("fleet_mean")
    )

    # Z-score (guard against zero std)
    enriched = enriched.withColumn(
        "z_score",
        F.when(
            F.col("fleet_std") > 0,
            F.col("deviation") / F.col("fleet_std"),
        ).otherwise(F.lit(0.0)),
    )

    # Anomaly threshold band
    enriched = enriched.withColumn(
        "threshold", F.lit(std_threshold) * F.col("fleet_std")
    )

    # Boolean flag
    enriched = enriched.withColumn(
        "is_anomaly",
        F.abs(F.col("deviation")) > F.col("threshold"),
    )

    # Severity score scaled by pi
    enriched = enriched.withColumn(
        "severity",
        F.when(
            (F.col("is_anomaly")) & (F.col("threshold") > 0),
            (F.abs(F.col("deviation")) / F.col("threshold"))
            * F.lit(ANOMALY_SEVERITY_SCALE),  # pi
        ).otherwise(F.lit(0.0)),
    )

    # Direction label
    enriched = enriched.withColumn(
        "anomaly_direction",
        F.when(
            (F.col("is_anomaly")) & (F.col("deviation") > 0), F.lit("OVER")
        ).when(
            (F.col("is_anomaly")) & (F.col("deviation") < 0), F.lit("UNDER")
        ).otherwise(F.lit("NORMAL")),
    )

    anomaly_count = enriched.filter(F.col("is_anomaly")).count()
    total_count = enriched.count()
    logger.info(
        "Anomaly detection complete: %d anomalies out of %d turbine-windows",
        anomaly_count, total_count,
    )
    return enriched


def get_anomalies_only(enriched_df: DataFrame) -> DataFrame:
    """Return only the anomalous turbine-window rows.

    Args:
        enriched_df: Output of ``detect_anomalies``.

    Returns:
        Filtered DataFrame containing only anomalous entries.
    """
    return enriched_df.filter(F.col("is_anomaly"))
