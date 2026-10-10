"""Nebula Storage Module - Delta table persistence for the Andromeda Wind Pipeline.

Writes processed DataFrames to Delta tables in Unity Catalog (or local
Delta format for standalone / test runs).  Supports both batch writes
and incremental merge-upserts.
"""

from __future__ import annotations

import logging
from typing import List, Optional

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from config.pipeline_config import (
    DELTA_CATALOG,
    DELTA_SCHEMA,
    TABLE_ANOMALIES,
    TABLE_CLEANED,
    TABLE_RAW,
    TABLE_SUMMARY,
    get_fqn,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Schema / catalog initialisation
# ---------------------------------------------------------------------------

def ensure_schema_exists(
    spark: SparkSession,
    catalog: str = DELTA_CATALOG,
    schema: str = DELTA_SCHEMA,
) -> None:
    """Create catalog and schema if they don't already exist.

    Args:
        spark: Active SparkSession.
        catalog: Unity Catalog catalog name.
        schema: Schema (database) name.
    """
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    logger.info("Ensured catalog=%s schema=%s", catalog, schema)


# ---------------------------------------------------------------------------
# Generic Delta writers
# ---------------------------------------------------------------------------

def write_delta(
    df: DataFrame,
    table_name: str,
    mode: str = "overwrite",
    partition_by: Optional[List[str]] = None,
    catalog: str = DELTA_CATALOG,
    schema: str = DELTA_SCHEMA,
) -> str:
    """Write a DataFrame to a Delta table.

    Args:
        df: DataFrame to persist.
        table_name: Short table name (no catalog/schema prefix).
        mode: Spark write mode ('overwrite' or 'append').
        partition_by: Optional list of partition columns.
        catalog: Target catalog.
        schema: Target schema.

    Returns:
        Fully qualified table name that was written.
    """
    fqn = get_fqn(table_name, catalog, schema)
    writer = df.write.format("delta").mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.saveAsTable(fqn)
    row_count = df.count()
    logger.info("Wrote %d rows to %s (mode=%s)", row_count, fqn, mode)
    return fqn


def write_delta_path(
    df: DataFrame,
    path: str,
    mode: str = "overwrite",
    partition_by: Optional[List[str]] = None,
) -> str:
    """Write a DataFrame to a Delta path (for standalone / testing).

    Args:
        df: DataFrame to persist.
        path: File-system path for Delta output.
        mode: Spark write mode.
        partition_by: Optional partition columns.

    Returns:
        The path written to.
    """
    writer = df.write.format("delta").mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)
    logger.info("Wrote Delta to path: %s (mode=%s)", path, mode)
    return path


# ---------------------------------------------------------------------------
# Pipeline-specific writers
# ---------------------------------------------------------------------------

def store_raw_data(df: DataFrame, **kwargs) -> str:
    """Persist raw (pre-cleaning) turbine readings."""
    return write_delta(df, TABLE_RAW, partition_by=["source_file"], **kwargs)


def store_cleaned_data(df: DataFrame, **kwargs) -> str:
    """Persist cleaned turbine readings."""
    return write_delta(df, TABLE_CLEANED, partition_by=["source_file"], **kwargs)


def store_summary_statistics(df: DataFrame, **kwargs) -> str:
    """Persist per-turbine summary statistics."""
    return write_delta(df, TABLE_SUMMARY, **kwargs)


def store_anomalies(df: DataFrame, **kwargs) -> str:
    """Persist detected anomalies."""
    return write_delta(df, TABLE_ANOMALIES, **kwargs)


# ---------------------------------------------------------------------------
# Convenience: store all pipeline outputs
# ---------------------------------------------------------------------------

def store_all(
    raw_df: DataFrame,
    cleaned_df: DataFrame,
    summary_df: DataFrame,
    anomalies_df: DataFrame,
    catalog: str = DELTA_CATALOG,
    schema: str = DELTA_SCHEMA,
) -> dict:
    """Persist every pipeline artefact to Delta.

    Args:
        raw_df: Raw ingested data.
        cleaned_df: Cleaned data.
        summary_df: Summary statistics.
        anomalies_df: Anomaly-enriched data.
        catalog: Target catalog.
        schema: Target schema.

    Returns:
        Dict mapping logical name -> fully qualified table name.
    """
    kw = dict(catalog=catalog, schema=schema)
    return {
        "raw": store_raw_data(raw_df, **kw),
        "cleaned": store_cleaned_data(cleaned_df, **kw),
        "summary": store_summary_statistics(summary_df, **kw),
        "anomalies": store_anomalies(anomalies_df, **kw),
    }
