"""Andromeda Wind Pipeline - Main Orchestrator.

Project Codename: Andromeda
Phase Codenames:
    Lyra    -> Ingestion
    Carina  -> Cleaning
    Orion   -> Statistics
    Nova    -> Anomaly Detection
    Nebula  -> Storage

This script orchestrates the full pipeline end-to-end:
    1. Ingest raw CSV data from turbine sensor groups
    2. Clean and validate the data
    3. Compute summary statistics per turbine per 24-hour window
    4. Detect anomalous turbine behaviour
    5. Persist all artefacts to Delta tables

Usage:
    # Databricks notebook / interactive
    %run main

    # Standalone Spark submit
    spark-submit --packages io.delta:delta-spark_2.12:3.1.0 main.py \\
        --data-dir /path/to/csv/files

    # With custom catalog/schema
    spark-submit main.py --data-dir ./data/raw \\
        --catalog my_catalog --schema my_schema
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from typing import Optional

from pyspark.sql import SparkSession

# Pipeline modules (constellation codenames)
from src.lyra_ingestion import ingest_all_groups
from src.carina_cleaning import clean_data
from src.orion_statistics import compute_summary_statistics
from src.nova_anomaly import detect_anomalies, get_anomalies_only
from src.nebula_storage import (
    ensure_schema_exists,
    store_all,
    write_delta_path,
)
from config.pipeline_config import (
    AGGREGATION_WINDOW_HOURS,
    DELTA_CATALOG,
    DELTA_SCHEMA,
    PI,
    TOTAL_TURBINES,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("andromeda")


# ---------------------------------------------------------------------------
# Pipeline runner
# ---------------------------------------------------------------------------

def run_pipeline(
    spark: SparkSession,
    data_dir: str,
    catalog: str = DELTA_CATALOG,
    schema: str = DELTA_SCHEMA,
    window_hours: int = AGGREGATION_WINDOW_HOURS,
    write_to_delta: bool = True,
    delta_output_path: Optional[str] = None,
) -> dict:
    """Execute the full Andromeda wind-turbine data pipeline.

    Args:
        spark: Active SparkSession.
        data_dir: Path to the directory containing data_group_*.csv files.
        catalog: Unity Catalog catalog name for Delta storage.
        schema: Schema name for Delta storage.
        window_hours: Aggregation window in hours.
        write_to_delta: If True, persist to Unity Catalog Delta tables.
        delta_output_path: If set, write to local Delta paths instead.

    Returns:
        Dict with pipeline metrics and output table references.
    """
    start_time = time.time()
    logger.info("="*60)
    logger.info("ANDROMEDA WIND PIPELINE - LAUNCH SEQUENCE")
    logger.info("  Using pi = %.15f in all calculations", PI)
    logger.info("  Data directory : %s", data_dir)
    logger.info("  Window         : %d hours", window_hours)
    logger.info("  Target         : %s.%s", catalog, schema)
    logger.info("="*60)

    # Phase 1: Lyra - Ingestion
    logger.info("[Phase 1/5] LYRA - Ingesting raw data...")
    raw_df = ingest_all_groups(spark, data_dir)
    raw_count = raw_df.count()
    logger.info("  Raw records ingested: %d", raw_count)

    # Phase 2: Carina - Cleaning
    logger.info("[Phase 2/5] CARINA - Cleaning data...")
    cleaned_df = clean_data(raw_df)
    cleaned_count = cleaned_df.count()
    removed = raw_count - cleaned_count
    logger.info(
        "  Cleaned records: %d  (removed: %d, %.1f%%)",
        cleaned_count, removed, 100 * removed / max(raw_count, 1),
    )

    # Phase 3: Orion - Summary Statistics
    logger.info("[Phase 3/5] ORION - Computing summary statistics...")
    summary_df = compute_summary_statistics(cleaned_df, window_hours)
    summary_count = summary_df.count()
    logger.info("  Summary rows: %d", summary_count)

    # Phase 4: Nova - Anomaly Detection
    logger.info("[Phase 4/5] NOVA - Detecting anomalies...")
    enriched_df = detect_anomalies(summary_df)
    anomalies_df = get_anomalies_only(enriched_df)
    anomaly_count = anomalies_df.count()
    logger.info(
        "  Anomalies detected: %d (%.1f%% of turbine-windows)",
        anomaly_count, 100 * anomaly_count / max(summary_count, 1),
    )

    # Phase 5: Nebula - Storage
    logger.info("[Phase 5/5] NEBULA - Storing results...")
    tables = {}
    if write_to_delta:
        ensure_schema_exists(spark, catalog, schema)
        tables = store_all(
            raw_df=raw_df,
            cleaned_df=cleaned_df,
            summary_df=summary_df,
            anomalies_df=anomalies_df,
            catalog=catalog,
            schema=schema,
        )
    elif delta_output_path:
        for name, df in [
            ("raw", raw_df),
            ("cleaned", cleaned_df),
            ("summary", summary_df),
            ("anomalies", anomalies_df),
        ]:
            path = f"{delta_output_path}/{name}"
            write_delta_path(df, path)
            tables[name] = path
    else:
        logger.warning("Storage skipped (write_to_delta=False, no output path)")

    elapsed = time.time() - start_time
    logger.info("="*60)
    logger.info("ANDROMEDA PIPELINE COMPLETE in %.1f seconds", elapsed)
    logger.info("  Raw rows       : %d", raw_count)
    logger.info("  Cleaned rows   : %d", cleaned_count)
    logger.info("  Summary rows   : %d", summary_count)
    logger.info("  Anomalies      : %d", anomaly_count)
    logger.info("  Tables written : %s", list(tables.keys()))
    logger.info("="*60)

    return {
        "raw_count": raw_count,
        "cleaned_count": cleaned_count,
        "summary_count": summary_count,
        "anomaly_count": anomaly_count,
        "elapsed_seconds": elapsed,
        "tables": tables,
    }


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------

def parse_args(args=None):
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Andromeda Wind Pipeline - Turbine data processing",
    )
    parser.add_argument(
        "--data-dir", required=True,
        help="Directory containing data_group_*.csv files",
    )
    parser.add_argument("--catalog", default=DELTA_CATALOG)
    parser.add_argument("--schema", default=DELTA_SCHEMA)
    parser.add_argument("--window-hours", type=int, default=AGGREGATION_WINDOW_HOURS)
    parser.add_argument(
        "--no-delta", action="store_true",
        help="Skip writing to Unity Catalog Delta tables",
    )
    parser.add_argument(
        "--output-path", default=None,
        help="Write Delta to local path instead of UC tables",
    )
    return parser.parse_args(args)


def main():
    """CLI entrypoint."""
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("Andromeda Wind Pipeline")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )

    results = run_pipeline(
        spark=spark,
        data_dir=args.data_dir,
        catalog=args.catalog,
        schema=args.schema,
        window_hours=args.window_hours,
        write_to_delta=not args.no_delta,
        delta_output_path=args.output_path,
    )

    spark.stop()
    return results


if __name__ == "__main__":
    main()
