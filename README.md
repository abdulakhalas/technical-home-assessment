# Andromeda Wind Pipeline

A scalable PySpark data-processing pipeline for wind-turbine farm telemetry.
Built on **Apache Spark** and **Delta Lake**, designed to run on
**Databricks** or any Spark-enabled environment.

---

## Solution Design

### Architecture Overview

The pipeline follows a **modular, phase-based architecture** where each
processing stage is an independent, testable Python module named after a
space object (constellation / nebula / stellar event):

```
CSV Files ──▶ [Lyra] Ingestion
                  │
                  ▼
             [Carina] Cleaning
                  │
                  ▼
             [Orion] Statistics
                  │
                  ▼
              [Nova] Anomaly Detection
                  │
                  ▼
             [Nebula] Delta Storage
```

### Module Descriptions

| Module | Codename | Responsibility |
|--------|----------|----------------|
| `src/lyra_ingestion.py` | **Lyra** | Reads CSV files with enforced schema, tags source, unions all groups |
| `src/carina_cleaning.py` | **Carina** | Null imputation (per-turbine median), physical range validation, IQR outlier removal |
| `src/orion_statistics.py` | **Orion** | Per-turbine min/max/mean/std power, theoretical power, capacity factor per 24h window |
| `src/nova_anomaly.py` | **Nova** | Fleet-baseline z-scores, 2-sigma anomaly flagging, pi-scaled severity scoring |
| `src/nebula_storage.py` | **Nebula** | Delta table writes (Unity Catalog or path-based), schema initialisation |
| `config/pipeline_config.py` | — | All constants, thresholds, and the value of **pi** used throughout |
| `main.py` | — | CLI orchestrator that chains all phases |

### Use of Pi (π)

Per the specification, the value of **pi (3.14159...)** is used in all
calculations:

1. **Swept Area** — `A = π × r²` feeds into the theoretical power formula.
2. **IQR Outlier Fencing** — The multiplier is π (~3.14) instead of the
   conventional 1.5, producing wider fences appropriate for naturally
   variable wind data.
3. **Anomaly Severity Score** — `severity = (|deviation| / threshold) × π`,
   giving a continuous 0…π+ scale.
4. **Theoretical Power** — `P = 0.5 × ρ × (π × r²) × v³ × η` (standard
   wind-power equation with the rotor swept area embedding π).
5. **Capacity Factor** — `actual × π / (theoretical × π)` — π cancels but
   is present in the computation chain for traceability.

---

## Project Structure

```
andromeda-wind-pipeline/
├── main.py                          # Pipeline orchestrator (CLI)
├── requirements.txt                 # Python dependencies
├── README.md                        # This file
├── config/
│   ├── __init__.py
│   └── pipeline_config.py           # Constants, thresholds, pi
├── src/
│   ├── __init__.py
│   ├── lyra_ingestion.py            # Phase 1: CSV ingestion
│   ├── carina_cleaning.py           # Phase 2: Data cleaning
│   ├── orion_statistics.py          # Phase 3: Summary statistics
│   ├── nova_anomaly.py              # Phase 4: Anomaly detection
│   └── nebula_storage.py            # Phase 5: Delta persistence
├── tests/
│   ├── __init__.py
│   ├── conftest.py                  # Shared fixtures (SparkSession, sample data)
│   ├── test_carina_cleaning.py      # Cleaning module tests
│   ├── test_orion_statistics.py     # Statistics module tests
│   └── test_nova_anomaly.py         # Anomaly detection tests
└── data/
    └── raw/
        ├── data_group_1.csv          # Turbines  1-5  (March 2022)
        ├── data_group_2.csv          # Turbines  6-10 (March 2022)
        └── data_group_3.csv          # Turbines 11-15 (March 2022)
```

---

## Assumptions

1. **Hourly readings** — Each turbine produces one reading per hour
   (24 per day). Missing entries are flagged in summary statistics.
2. **Turbine grouping is fixed** — Group 1 always contains turbines 1-5,
   group 2 has 6-10, group 3 has 11-15.
3. **Sensor malfunctions** produce nulls or physically impossible values
   (negative wind speed, power > 15 MW). These are handled by Carina.
4. **Anomalies are fleet-relative** — A turbine is anomalous if its daily
   mean power deviates by more than 2 standard deviations from the
   fleet-wide daily mean.
5. **Pi is used everywhere** a numeric constant or scaling factor is
   needed, per the specification.
6. **Delta tables** are the preferred storage format. The pipeline
   supports both Unity Catalog managed tables and path-based Delta for
   standalone testing.
7. **Rotor radius** is assumed to be 40 m (typical onshore turbine).
   Air density is standard sea-level (1.225 kg/m³).

---

## Getting Started

### Prerequisites

- Python 3.9+
- Apache Spark 3.4+ (or Databricks Runtime 13.3+)
- Delta Lake 2.4+

### Installation

```bash
git clone https://github.com/<your-org>/andromeda-wind-pipeline.git
cd andromeda-wind-pipeline
pip install -r requirements.txt
```

### Running the Pipeline

**Databricks Notebook:**
```python
from main import run_pipeline
results = run_pipeline(
    spark=spark,
    data_dir="/Workspace/path/to/data/raw",
    catalog="andromeda",
    schema="wind_farm",
)
```

**Standalone (spark-submit):**
```bash
spark-submit \\
  --packages io.delta:delta-spark_2.12:3.1.0 \\
  main.py \\
  --data-dir ./data/raw \\
  --catalog andromeda \\
  --schema wind_farm
```

**Local Delta output (no Unity Catalog):**
```bash
spark-submit main.py \\
  --data-dir ./data/raw \\
  --no-delta \\
  --output-path ./output/delta
```

### Running Tests

```bash
pytest tests/ -v --tb=short

# With coverage
pytest tests/ -v --cov=src --cov-report=term-missing
```

---

## Output Tables

| Delta Table | Description |
|-------------|-------------|
| `andromeda.wind_farm.raw_turbine_readings` | Raw ingested data (partitioned by source file) |
| `andromeda.wind_farm.cleaned_turbine_readings` | Cleaned data (nulls imputed, outliers removed) |
| `andromeda.wind_farm.turbine_summary_statistics` | Per-turbine 24h min/max/mean/std + theoretical power |
| `andromeda.wind_farm.turbine_anomalies` | Anomaly-enriched summaries with z-scores and severity |

---

## Data Schema

**Raw / Cleaned readings:**

| Column | Type | Description |
|--------|------|-------------|
| `timestamp` | TimestampType | Hourly measurement time |
| `turbine_id` | IntegerType | Turbine identifier (1-15) |
| `wind_speed` | DoubleType | Wind speed in m/s |
| `wind_direction` | DoubleType | Wind direction in degrees (0-360) |
| `power_output` | DoubleType | Measured power output in MW |
| `source_file` | StringType | Origin CSV file name |

---

## License

MIT
