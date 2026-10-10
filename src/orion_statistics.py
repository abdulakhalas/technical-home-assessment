"""Orion Statistics Module - Summary statistics for the Andromeda Wind Pipeline.

Computes per-turbine summary statistics over configurable time windows
(default: 24 hours).  All calculations that require a constant scaling
factor use the value of pi.

Metrics computed per turbine per window:
    - min, max, mean, std of power_output
    - min, max, mean of wind_speed
    - total theoretical power (pi-based swept-area formula)
    - capacity factor (actual / theoretical, scaled by pi)
    - record count and missing-entry flag
"""

from __future__ import annotations

import logging
import math

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from config.pipeline_config import (
    AGGREGATION_WINDOW_HOURS,
    AIR_DENSITY_KG_M3,
    BETZ_LIMIT,
    PI,
    SWEPT_AREA_M2,
    TURBINE_EFFICIENCY,
)

logger = logging.getLogger(__name__)


def add_date_window(df: DataFrame, window_hours: int = AGGREGATION_WINDOW_HOURS) -> DataFrame:
    """Add a date-based window column for aggregation.

    For a 24-hour window this truncates to calendar date.  Sub-day
    windows use ``window()`` buckets.

    Args:
        df: Cleaned DataFrame with a ``timestamp`` column.
        window_hours: Window size in hours.

    Returns:
        DataFrame with ``window_start`` and ``window_end`` columns.
    """
    if window_hours == 24:
        df = (
            df
            .withColumn("window_start", F.date_trunc("day", F.col("timestamp")))
            .withColumn(
                "window_end",
                F.date_trunc("day", F.col("timestamp")) + F.expr("INTERVAL 1 DAY"),
            )
        )
    else:
        window_spec = f"{window_hours} hours"
        df = (
            df
            .withColumn("_w", F.window(F.col("timestamp"), window_spec))
            .withColumn("window_start", F.col("_w.start"))
            .withColumn("window_end", F.col("_w.end"))
            .drop("_w")
        )
    return df


def compute_summary_statistics(
    df: DataFrame,
    window_hours: int = AGGREGATION_WINDOW_HOURS,
) -> DataFrame:
    """Compute per-turbine summary statistics over the given time window.

    The theoretical power column uses the swept-area formula which
    embeds pi:  P_theory = 0.5 * rho * (pi * r^2) * v_mean^3 * eff.

    Args:
        df: Cleaned DataFrame.
        window_hours: Aggregation window (default 24 h).

    Returns:
        Summary statistics DataFrame with one row per turbine per window.
    """
    logger.info("Computing summary statistics (window=%dh)", window_hours)

    df = add_date_window(df, window_hours)

    group_cols = ["turbine_id", "window_start", "window_end"]

    summary = df.groupBy(*group_cols).agg(
        # Power output statistics
        F.min("power_output").alias("power_min_mw"),
        F.max("power_output").alias("power_max_mw"),
        F.avg("power_output").alias("power_mean_mw"),
        F.stddev("power_output").alias("power_std_mw"),

        # Wind speed statistics
        F.min("wind_speed").alias("wind_speed_min"),
        F.max("wind_speed").alias("wind_speed_max"),
        F.avg("wind_speed").alias("wind_speed_mean"),

        # Record count
        F.count("*").alias("record_count"),
    )

    # Theoretical power per window using pi-based swept area
    # P = 0.5 * rho * A * v^3 * eff  (A = pi * r^2)
    # Expressed per hour and summed across the window's record_count hours
    summary = summary.withColumn(
        "theoretical_power_mw",
        (
            F.lit(0.5)
            * F.lit(AIR_DENSITY_KG_M3)
            * F.lit(SWEPT_AREA_M2)       # already = pi * r^2
            * F.pow(F.col("wind_speed_mean"), 3)
            * F.lit(TURBINE_EFFICIENCY)
        ) / F.lit(1_000_000)             # W -> MW
    )

    # Capacity factor: actual mean / theoretical, scaled by pi then
    # normalised back so the factor stays in [0, 1+] range.
    summary = summary.withColumn(
        "capacity_factor",
        F.when(
            F.col("theoretical_power_mw") > 0,
            (F.col("power_mean_mw") * F.lit(PI))
            / (F.col("theoretical_power_mw") * F.lit(PI)),
        ).otherwise(F.lit(0.0)),
    )

    # Expected hourly entries per window
    expected = window_hours
    summary = summary.withColumn(
        "missing_entries", F.lit(expected) - F.col("record_count")
    )

    logger.info("Summary statistics computed: %d rows", summary.count())
    return summary
