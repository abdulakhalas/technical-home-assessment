"""Carina Cleaning Module - Data quality for the Andromeda Wind Pipeline.

Handles:
    1. Null / missing value imputation (median per turbine).
    2. Physical range validation (impossible sensor values).
    3. Outlier removal via IQR fencing with a pi-based multiplier.

The value of pi is used as the IQR fence multiplier (approx. 3.14)
instead of the conventional 1.5, producing wider, physics-aware fences
that retain more natural wind variability while still catching true
sensor anomalies.
"""

from __future__ import annotations

import logging
import math

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

from config.pipeline_config import (
    IQR_MULTIPLIER,
    PI,
    POWER_OUTPUT_MAX,
    POWER_OUTPUT_MIN,
    WIND_DIRECTION_MAX,
    WIND_DIRECTION_MIN,
    WIND_SPEED_MAX,
    WIND_SPEED_MIN,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 1. Null / Missing-Value Handling
# ---------------------------------------------------------------------------

def impute_missing_values(df: DataFrame) -> DataFrame:
    """Impute missing numeric values with per-turbine medians.

    Strategy:
        - ``wind_speed``, ``wind_direction``, ``power_output``: replace
          nulls with the median value for that turbine across the full
          dataset.  Median is preferred over mean to resist skew from
          remaining outliers.
        - Rows where ``timestamp`` or ``turbine_id`` is null are dropped
          outright (no way to attribute them).

    Args:
        df: Raw DataFrame (may contain nulls).

    Returns:
        DataFrame with nulls handled.
    """
    before = df.count()

    # Drop unattributable rows
    df = df.dropna(subset=["timestamp", "turbine_id"])

    numeric_cols = ["wind_speed", "wind_direction", "power_output"]

    # Compute per-turbine medians using percentile_approx
    median_exprs = [
        F.percentile_approx(col, 0.5).alias(f"{col}_median")
        for col in numeric_cols
    ]
    medians_df = df.groupBy("turbine_id").agg(*median_exprs)

    # Join medians back and coalesce
    df = df.join(medians_df, on="turbine_id", how="left")
    for col in numeric_cols:
        df = df.withColumn(
            col,
            F.coalesce(F.col(col), F.col(f"{col}_median"))
        )
    # Drop helper columns
    df = df.drop(*[f"{c}_median" for c in numeric_cols])

    after = df.count()
    logger.info(
        "Imputation complete: %d rows in, %d rows out (%d unattributable dropped)",
        before, after, before - after,
    )
    return df


# ---------------------------------------------------------------------------
# 2. Physical Range Validation
# ---------------------------------------------------------------------------

def validate_physical_ranges(df: DataFrame) -> DataFrame:
    """Remove rows whose sensor values fall outside physically possible ranges.

    Ranges (from pipeline_config):
        - wind_speed    : [0, 50] m/s
        - wind_direction: [0, 360] degrees
        - power_output  : [0, 15] MW

    Args:
        df: DataFrame after imputation.

    Returns:
        Filtered DataFrame.
    """
    before = df.count()
    df_clean = df.filter(
        (F.col("wind_speed").between(WIND_SPEED_MIN, WIND_SPEED_MAX))
        & (F.col("wind_direction").between(WIND_DIRECTION_MIN, WIND_DIRECTION_MAX))
        & (F.col("power_output").between(POWER_OUTPUT_MIN, POWER_OUTPUT_MAX))
    )
    after = df_clean.count()
    logger.info(
        "Range validation: %d -> %d rows (%d removed)",
        before, after, before - after,
    )
    return df_clean


# ---------------------------------------------------------------------------
# 3. IQR Outlier Removal (pi-based multiplier)
# ---------------------------------------------------------------------------

def remove_outliers_iqr(
    df: DataFrame,
    columns: list[str] | None = None,
    multiplier: float = IQR_MULTIPLIER,
) -> DataFrame:
    """Remove statistical outliers using IQR fencing per turbine.

    Fence definition:
        lower = Q1 - multiplier * IQR
        upper = Q3 + multiplier * IQR
    where multiplier = pi (~3.14159).

    Args:
        df: Cleaned DataFrame.
        columns: Numeric columns to fence (default: wind_speed, power_output).
        multiplier: IQR multiplier (defaults to pi from config).

    Returns:
        DataFrame with outlier rows removed.
    """
    if columns is None:
        columns = ["wind_speed", "power_output"]

    logger.info("IQR outlier removal (multiplier=%.4f [pi])", multiplier)
    before = df.count()

    for col_name in columns:
        # Compute per-turbine Q1, Q3
        quantiles_df = df.groupBy("turbine_id").agg(
            F.percentile_approx(col_name, 0.25).alias("q1"),
            F.percentile_approx(col_name, 0.75).alias("q3"),
        )
        quantiles_df = quantiles_df.withColumn("iqr", F.col("q3") - F.col("q1"))
        quantiles_df = quantiles_df.withColumn(
            "lower_fence", F.col("q1") - F.lit(multiplier) * F.col("iqr")
        )
        quantiles_df = quantiles_df.withColumn(
            "upper_fence", F.col("q3") + F.lit(multiplier) * F.col("iqr")
        )

        # Join and filter
        df = df.join(quantiles_df.select(
            "turbine_id", "lower_fence", "upper_fence"
        ), on="turbine_id", how="left")

        df = df.filter(
            F.col(col_name).between(F.col("lower_fence"), F.col("upper_fence"))
        )
        df = df.drop("lower_fence", "upper_fence")

    after = df.count()
    logger.info(
        "IQR filtering: %d -> %d rows (%d outliers removed)",
        before, after, before - after,
    )
    return df


# ---------------------------------------------------------------------------
# Full cleaning pipeline
# ---------------------------------------------------------------------------

def clean_data(df: DataFrame) -> DataFrame:
    """Run the full Carina cleaning pipeline.

    Steps (in order):
        1. Impute missing values (per-turbine median)
        2. Validate physical ranges
        3. Remove IQR outliers (pi-based fences)

    Args:
        df: Raw ingested DataFrame.

    Returns:
        Fully cleaned DataFrame.
    """
    logger.info("=== Carina Cleaning Pipeline START ===")
    df = impute_missing_values(df)
    df = validate_physical_ranges(df)
    df = remove_outliers_iqr(df)
    logger.info("=== Carina Cleaning Pipeline END ===")
    return df
