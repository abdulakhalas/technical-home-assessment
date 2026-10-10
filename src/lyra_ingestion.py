"""Lyra Ingestion Module - Data ingestion for the Andromeda Wind Pipeline.

Reads CSV files produced by turbine sensor groups, enforces a strict
schema, and unions them into a single Spark DataFrame ready for
downstream processing.

Each CSV contains hourly readings for a group of 5 turbines:
    data_group_1.csv -> turbines  1-5
    data_group_2.csv -> turbines  6-10
    data_group_3.csv -> turbines 11-15
"""

from __future__ import annotations

import logging
from functools import reduce
from pathlib import Path
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
import pyspark.sql.functions as F

logger = logging.getLogger(__name__)

# Strict schema enforced on every CSV read
TURBINE_SCHEMA = StructType([
    StructField("timestamp", TimestampType(), nullable=True),
    StructField("turbine_id", IntegerType(), nullable=True),
    StructField("wind_speed", DoubleType(), nullable=True),
    StructField("wind_direction", DoubleType(), nullable=True),
    StructField("power_output", DoubleType(), nullable=True),
])


def read_single_csv(
    spark: SparkSession,
    file_path: str,
    schema: StructType = TURBINE_SCHEMA,
) -> DataFrame:
    """Read a single turbine CSV with an enforced schema.

    Args:
        spark: Active SparkSession.
        file_path: Absolute or relative path to the CSV file.
        schema: Expected schema; mismatched columns become null.

    Returns:
        DataFrame with the enforced schema plus a ``source_file`` column.
    """
    logger.info("Reading CSV: %s", file_path)
    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(schema)
        .load(file_path)
    )
    # Tag each row with its source file for traceability
    source_name = Path(file_path).name
    df = df.withColumn("source_file", F.lit(source_name))
    row_count = df.count()
    logger.info("  -> %d rows ingested from %s", row_count, source_name)
    return df


def ingest_all_groups(
    spark: SparkSession,
    data_dir: str,
    group_ids: Optional[List[int]] = None,
    file_pattern: str = "data_group_{group_id}.csv",
) -> DataFrame:
    """Ingest all CSV groups and union into a single DataFrame.

    Args:
        spark: Active SparkSession.
        data_dir: Directory containing the CSV files.
        group_ids: List of group IDs to load (default: [1, 2, 3]).
        file_pattern: File name pattern with ``{group_id}`` placeholder.

    Returns:
        Unioned DataFrame with all turbine readings.
    """
    if group_ids is None:
        group_ids = [1, 2, 3]

    dfs: List[DataFrame] = []
    for gid in group_ids:
        file_name = file_pattern.format(group_id=gid)
        full_path = f"{data_dir}/{file_name}"
        dfs.append(read_single_csv(spark, full_path))

    if not dfs:
        raise ValueError("No data files were loaded. Check data_dir and group_ids.")

    combined = reduce(DataFrame.unionByName, dfs)
    total = combined.count()
    logger.info("Total rows after union: %d", total)
    return combined
