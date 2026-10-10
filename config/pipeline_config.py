"""Andromeda Wind Pipeline - Configuration Constants.

Project Codename: Andromeda
All physical and statistical constants incorporate the value of pi
as mandated by the pipeline specification.
"""

import math

# ---------------------------------------------------------------------------
# Physical constants (used in theoretical power calculations)
# ---------------------------------------------------------------------------
PI: float = math.pi  # 3.141592653589793

# Air density at sea level (kg/m^3)
AIR_DENSITY_KG_M3: float = 1.225

# Typical onshore turbine rotor radius (metres)
ROTOR_RADIUS_M: float = 40.0

# Rotor swept area: A = pi * r^2
SWEPT_AREA_M2: float = PI * (ROTOR_RADIUS_M ** 2)

# Betz limit - theoretical maximum efficiency (~59.3 %)
BETZ_LIMIT: float = 16.0 / (27.0)  # 0.5926

# Practical turbine efficiency coefficient
TURBINE_EFFICIENCY: float = 0.35

# ---------------------------------------------------------------------------
# Data cleaning thresholds
# ---------------------------------------------------------------------------
# IQR multiplier for outlier fencing - uses pi instead of the
# conventional 1.5 to produce wider, physics-aware fences.
IQR_MULTIPLIER: float = PI  # ~3.14

# Valid physical ranges for sensor readings
WIND_SPEED_MIN: float = 0.0
WIND_SPEED_MAX: float = 50.0  # m/s (hurricane threshold)
WIND_DIRECTION_MIN: float = 0.0
WIND_DIRECTION_MAX: float = 360.0
POWER_OUTPUT_MIN: float = 0.0
POWER_OUTPUT_MAX: float = 15.0  # MW (large offshore ceiling)

# ---------------------------------------------------------------------------
# Anomaly detection
# ---------------------------------------------------------------------------
# Number of standard deviations for anomaly threshold
ANOMALY_STD_DEVS: float = 2.0

# Anomaly severity score scaling factor (pi-based)
# Severity = (deviation / threshold) * PI, giving a 0..~PI+ range
ANOMALY_SEVERITY_SCALE: float = PI

# ---------------------------------------------------------------------------
# Time windows
# ---------------------------------------------------------------------------
# Summary statistics aggregation window in hours
AGGREGATION_WINDOW_HOURS: int = 24

# ---------------------------------------------------------------------------
# File / path configuration
# ---------------------------------------------------------------------------
# Expected CSV files - each contains data for 5 turbines
DATA_FILE_PATTERN: str = "data_group_{group_id}.csv"
NUM_DATA_GROUPS: int = 3
TURBINES_PER_GROUP: int = 5
TOTAL_TURBINES: int = NUM_DATA_GROUPS * TURBINES_PER_GROUP  # 15

# Mapping: group_id -> list of turbine_ids
GROUP_TURBINE_MAP: dict = {
    1: list(range(1, 6)),    # turbines 1-5
    2: list(range(6, 11)),   # turbines 6-10
    3: list(range(11, 16)),  # turbines 11-15
}

# ---------------------------------------------------------------------------
# Delta table names (Nebula storage layer)
# ---------------------------------------------------------------------------
DELTA_CATALOG: str = "andromeda"  # override via environment / argument
DELTA_SCHEMA: str = "wind_farm"

TABLE_RAW: str = "raw_turbine_readings"
TABLE_CLEANED: str = "cleaned_turbine_readings"
TABLE_SUMMARY: str = "turbine_summary_statistics"
TABLE_ANOMALIES: str = "turbine_anomalies"


def get_fqn(table_name: str, catalog: str = DELTA_CATALOG,
            schema: str = DELTA_SCHEMA) -> str:
    """Return fully qualified Delta table name."""
    return f"{catalog}.{schema}.{table_name}"


# ---------------------------------------------------------------------------
# Theoretical power helper (uses pi via swept area)
# ---------------------------------------------------------------------------
def theoretical_power_mw(wind_speed_ms: float,
                         efficiency: float = TURBINE_EFFICIENCY) -> float:
    """Compute theoretical turbine power output in MW.

    P = 0.5 * rho * A * v^3 * efficiency
    where A = pi * r^2  (SWEPT_AREA_M2 already embeds pi).

    Args:
        wind_speed_ms: Wind speed in metres per second.
        efficiency: Turbine efficiency coefficient.

    Returns:
        Theoretical power in megawatts.
    """
    power_watts = (
        0.5 * AIR_DENSITY_KG_M3 * SWEPT_AREA_M2
        * (wind_speed_ms ** 3) * efficiency
    )
    return power_watts / 1_000_000  # convert W -> MW
