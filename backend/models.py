"""
Data models for CloudPulse.
Uses Pydantic for cloud telemetry validation, OpenTelemetry compatibility,
and incident state modeling.
"""
import time
import uuid
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field


class TelemetryPoint(BaseModel):
    service_id: str
    region: str = "us-east-1"
    timestamp: float = Field(default_factory=time.time)
    request_count: int = Field(ge=0, default=100)
    error_count: int = Field(ge=0, default=0)
    latency_p95_ms: float = Field(ge=0.0, default=25.0)
    cpu_percent: float = Field(ge=0.0, le=100.0, default=30.0)
    memory_percent: float = Field(ge=0.0, le=100.0, default=45.0)
    trace_id: str = Field(default_factory=lambda: uuid.uuid4().hex[:16])
    correlation_id: str = Field(default_factory=lambda: f"corr-{uuid.uuid4().hex[:8]}")

    @property
    def error_rate_percent(self) -> float:
        if self.request_count == 0:
            return 0.0
        return round((self.error_count / self.request_count) * 100.0, 2)


class TelemetryBatch(BaseModel):
    batch_id: str = Field(default_factory=lambda: f"batch-{uuid.uuid4().hex[:8]}")
    points: List[TelemetryPoint]


class RemediationAction(BaseModel):
    action_id: str = Field(default_factory=lambda: f"rem-{uuid.uuid4().hex[:8]}")
    incident_id: str
    service_id: str
    action_type: str  # AUTO_SCALE, CIRCUIT_BREAKER_TRIP, RESTART_REPLICA, CACHE_PURGE, TRAFFIC_SHED
    details: str
    executed_at: float = Field(default_factory=time.time)
    success: bool = True
    parameters: Dict[str, Any] = Field(default_factory=dict)


class CloudIncident(BaseModel):
    incident_id: str = Field(default_factory=lambda: f"inc-{uuid.uuid4().hex[:8]}")
    title: str
    service_id: str
    severity: str = "MEDIUM"  # LOW, MEDIUM, HIGH, CRITICAL
    root_cause: str
    status: str = "DETECTED"  # DETECTED, HEALING_IN_PROGRESS, RESOLVED
    detected_at: float = Field(default_factory=time.time)
    resolved_at: Optional[float] = None
    trigger_metric: str
    trigger_value: float
    remediation_actions: List[RemediationAction] = Field(default_factory=list)


class ChaosInjectionRequest(BaseModel):
    target_service: str
    fault_type: str  # LATENCY_SPIKE, DB_POOL_EXHAUSTION, MEMORY_LEAK, HTTP_500_STORM
    duration_seconds: int = Field(default=20, ge=5, le=120)
    intensity: float = Field(default=1.0, ge=0.1, le=5.0)


class ServiceNodeState(BaseModel):
    service_id: str
    name: str
    tier: str  # frontend, core, storage, gateway
    status: str = "HEALTHY"  # HEALTHY, DEGRADED, CRITICAL, HEALING
    replicas: int = 2
    circuit_breaker_open: bool = False
    rate_limit_active: bool = False
    current_latency_ms: float = 25.0
    current_error_rate: float = 0.0
    current_cpu: float = 30.0
    current_memory: float = 40.0
    last_healed_at: Optional[float] = None
    last_heartbeat: float = Field(default_factory=time.time)
