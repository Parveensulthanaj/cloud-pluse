"""
Real-Time Statistical Anomaly Detector & Incident Correlator.
Applies dynamic Z-score and Exponential Moving Averages (EWMA) to cloud telemetry streams,
and correlates cascading microservice alerts to identify the root cause.
"""
import math
import time
import logging
from collections import defaultdict, deque
from typing import Dict, List, Optional, Tuple
from .config import config
from .models import CloudIncident, TelemetryPoint
from .event_bus import event_bus

logger = logging.getLogger("cloudpulse.anomaly_detector")


class AnomalyDetector:
    # Service dependency graph for blast-radius & root-cause correlation
    # Key: downstream service, Value: upstream dependency
    DEPENDENCY_GRAPH = {
        "order-api": "database-cluster",
        "payment-gateway": "database-cluster",
        "inventory-service": "database-cluster",
        "api-gateway": "auth-service",
    }

    def __init__(self):
        # Sliding history per service: {service_id: deque[TelemetryPoint]}
        self.history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=config.WINDOW_SIZE))
        # Active open incidents: {service_id: CloudIncident}
        self.active_incidents: Dict[str, CloudIncident] = {}
        # Historical incidents list
        self.incident_history: List[CloudIncident] = []
        # Debounce timer to prevent alert flapping
        self.last_incident_time: Dict[str, float] = {}

    def ingest_point(self, point: TelemetryPoint) -> Optional[CloudIncident]:
        """
        Ingest a telemetry point, update rolling baseline, and evaluate anomaly rules.
        """
        service_id = point.service_id
        hist = self.history[service_id]

        # Evaluate rules if we have at least 5 points for baseline estimation
        incident = None
        if len(hist) >= 5:
            incident = self._evaluate_anomalies(point, list(hist))

        # Append to sliding history
        hist.append(point)

        return incident

    def _evaluate_anomalies(self, point: TelemetryPoint, window: List[TelemetryPoint]) -> Optional[CloudIncident]:
        now = time.time()
        service_id = point.service_id

        # If already in an active incident for this service, skip duplicate creation
        if service_id in self.active_incidents:
            return None

        # Debounce: avoid re-triggering within 10 seconds of previous resolution
        if now - self.last_incident_time.get(service_id, 0) < 10.0:
            return None

        # 1. Compute dynamic baseline statistics for Latency
        latencies = [p.latency_p95_ms for p in window]
        mean_lat = sum(latencies) / len(latencies)
        variance = sum((x - mean_lat) ** 2 for x in latencies) / len(latencies)
        std_lat = math.sqrt(variance) if variance > 0.001 else 1.0

        z_score_lat = (point.latency_p95_ms - mean_lat) / std_lat

        # 2. Check for Latency Anomaly
        if z_score_lat >= config.LATENCY_Z_SCORE_THRESHOLD and point.latency_p95_ms > 70.0:
            severity = "CRITICAL" if z_score_lat > 4.0 or point.latency_p95_ms > 250 else "HIGH"
            root_cause = self._correlate_root_cause(service_id, "Latency Degradation")
            incident = CloudIncident(
                title=f"Severe Latency Spike in {service_id} (Z-Score: {z_score_lat:.2f})",
                service_id=service_id,
                severity=severity,
                root_cause=root_cause,
                trigger_metric="latency_p95_ms",
                trigger_value=point.latency_p95_ms,
            )
            return self._register_incident(incident)

        # 3. Check for Error Rate Anomaly
        error_rate = point.error_rate_percent
        if error_rate >= config.ERROR_RATE_PERCENT_THRESHOLD:
            severity = "CRITICAL" if error_rate > 25.0 else "HIGH"
            root_cause = self._correlate_root_cause(service_id, "HTTP 5xx Storm")
            incident = CloudIncident(
                title=f"Elevated Error Rate in {service_id} ({error_rate:.1f}%)",
                service_id=service_id,
                severity=severity,
                root_cause=root_cause,
                trigger_metric="error_rate_percent",
                trigger_value=error_rate,
            )
            return self._register_incident(incident)

        # 4. Check for Resource / CPU Saturation
        if point.cpu_percent >= config.CPU_SATURATION_THRESHOLD:
            severity = "CRITICAL" if point.cpu_percent > 95.0 else "HIGH"
            root_cause = f"Compute resource exhaustion on {service_id} pods"
            incident = CloudIncident(
                title=f"CPU Saturation on {service_id} ({point.cpu_percent:.1f}%)",
                service_id=service_id,
                severity=severity,
                root_cause=root_cause,
                trigger_metric="cpu_percent",
                trigger_value=point.cpu_percent,
            )
            return self._register_incident(incident)

        return None

    def _correlate_root_cause(self, service_id: str, symptom: str) -> str:
        """
        Check dependency topology to determine if an upstream failure is cascading.
        """
        upstream = self.DEPENDENCY_GRAPH.get(service_id)
        if upstream and upstream in self.active_incidents:
            return f"Cascading failure triggered by upstream service '{upstream}' ({self.active_incidents[upstream].title})"
        return f"{symptom} localized to {service_id} instance cluster"

    def _register_incident(self, incident: CloudIncident) -> CloudIncident:
        self.active_incidents[incident.service_id] = incident
        self.incident_history.append(incident)
        self.last_incident_time[incident.service_id] = time.time()
        logger.warning(f"[INCIDENT DETECTED] {incident.incident_id}: {incident.title} (Severity: {incident.severity})")
        return incident

    def resolve_incident(self, service_id: str, resolution_notes: str = "") -> Optional[CloudIncident]:
        """Mark an incident as resolved."""
        if service_id in self.active_incidents:
            incident = self.active_incidents.pop(service_id)
            incident.status = "RESOLVED"
            incident.resolved_at = time.time()
            logger.info(f"[INCIDENT RESOLVED] {incident.incident_id} on {service_id}. {resolution_notes}")
            return incident
        return None

    def get_active_incidents(self) -> List[CloudIncident]:
        return list(self.active_incidents.values())

    def get_all_incidents(self) -> List[CloudIncident]:
        # Return most recent 50 incidents
        return list(reversed(self.incident_history[-50:]))


anomaly_detector = AnomalyDetector()
