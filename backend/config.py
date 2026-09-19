"""
CloudPulse Configuration
Defines operational thresholds, SLA targets, anomaly detection parameters,
and cloud environment simulation parameters.
"""
from pydantic import BaseModel
import os


class SystemConfig(BaseModel):
    # Service Information
    APP_NAME: str = "CloudPulse Autonomous SRE Platform"
    APP_VERSION: str = "1.0.0"
    ENVIRONMENT: str = os.getenv("CLOUDPULSE_ENV", "production-simulation")
    HOST: str = os.getenv("CLOUDPULSE_HOST", "0.0.0.0")
    PORT: int = int(os.getenv("CLOUDPULSE_PORT", "8000"))

    # Anomaly Detection Parameters
    LATENCY_Z_SCORE_THRESHOLD: float = 2.5      # Z-score flag threshold for latency
    ERROR_RATE_PERCENT_THRESHOLD: float = 8.0   # Percentage error rate threshold
    CPU_SATURATION_THRESHOLD: float = 85.0      # CPU utilization percentage threshold
    WINDOW_SIZE: int = 40                       # Sliding telemetry window per service

    # Autonomous Self-Healing Policies
    AUTO_HEALING_ENABLED: bool = True
    REMEDIATION_COOLDOWN_SECONDS: int = 15     # Cooldown per service between remediation actions
    MAX_REPLICA_SCALE: int = 10                 # Maximum auto-scale ceiling
    MIN_REPLICA_SCALE: int = 2                  # Base replica count

    # Telemetry Retention
    MAX_INCIDENTS_IN_MEMORY: int = 100
    MAX_EVENT_LOG_HISTORY: int = 250


config = SystemConfig()
