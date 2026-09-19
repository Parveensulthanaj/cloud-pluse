"""
Comprehensive Automated Test Suite for CloudPulse.
Tests Event Bus, Dead Letter Queue, Statistical Anomaly Detection,
Autonomous Self-Healing Controller, Prometheus Metrics, and API Endpoints.
"""
import asyncio
import pytest
import time
from starlette.testclient import TestClient

from backend.config import config
from backend.models import TelemetryPoint, CloudIncident, ChaosInjectionRequest
from backend.event_bus import EventBus
from backend.anomaly_detector import AnomalyDetector
from backend.self_healer import SelfHealingController


# ==========================================
# 1. Event Bus & DLQ Unit Tests
# ==========================================

def test_event_bus_pub_sub():
    async def _test():
        bus = EventBus()
        received = []

        async def sample_handler(topic, payload):
            received.append((topic, payload))

        bus.subscribe("test.topic", sample_handler)
        delivered = await bus.publish("test.topic", {"msg": "hello-cloud"})

        assert delivered is True
        assert len(received) == 1
        assert received[0][0] == "test.topic"
        assert received[0][1]["msg"] == "hello-cloud"

        stats = bus.get_stats()
        assert stats["metrics"]["published"] == 1
        assert stats["metrics"]["delivered"] == 1
        assert stats["dlq_size"] == 0

    asyncio.run(_test())


def test_event_bus_dlq_on_failure():
    async def _test():
        bus = EventBus()

        async def broken_handler(topic, payload):
            raise ValueError("Simulated downstream microservice crash")

        bus.subscribe("fragile.topic", broken_handler)
        delivered = await bus.publish("fragile.topic", {"payload": "test-data"}, retry_count=1)

        assert delivered is False
        stats = bus.get_stats()
        assert stats["dlq_size"] == 1
        assert stats["metrics"]["failed"] == 1

        dlq_items = bus.get_dlq_events()
        assert len(dlq_items) == 1
        assert "Simulated downstream microservice crash" in dlq_items[0]["error"]

    asyncio.run(_test())


# ==========================================
# 2. Anomaly Detection & Incident Tests
# ==========================================

def test_anomaly_detector_baseline_and_spike():
    detector = AnomalyDetector()
    service = "payment-gateway"

    # Feed 10 baseline points (20ms latency, 0 errors, 30% cpu)
    for _ in range(10):
        point = TelemetryPoint(
            service_id=service,
            latency_p95_ms=20.0,
            request_count=100,
            error_count=0,
            cpu_percent=30.0,
        )
        incident = detector.ingest_point(point)
        assert incident is None  # Baseline should not trigger incident

    # Feed a massive latency spike (350ms)
    spike_point = TelemetryPoint(
        service_id=service,
        latency_p95_ms=350.0,
        request_count=100,
        error_count=0,
        cpu_percent=32.0,
    )
    incident = detector.ingest_point(spike_point)

    assert incident is not None
    assert incident.service_id == service
    assert incident.trigger_metric == "latency_p95_ms"
    assert incident.trigger_value == 350.0
    assert incident.severity in ["HIGH", "CRITICAL"]

    # Deduplication test: another immediate spike should NOT produce a duplicate incident
    dup_point = TelemetryPoint(
        service_id=service,
        latency_p95_ms=360.0,
        request_count=100,
        error_count=0,
    )
    assert detector.ingest_point(dup_point) is None


def test_anomaly_detector_error_rate_threshold():
    detector = AnomalyDetector()
    service = "auth-service"

    for _ in range(6):
        detector.ingest_point(TelemetryPoint(
            service_id=service,
            latency_p95_ms=15.0,
            request_count=100,
            error_count=0,
        ))

    # 40% error rate
    error_point = TelemetryPoint(
        service_id=service,
        latency_p95_ms=20.0,
        request_count=100,
        error_count=40,
    )
    incident = detector.ingest_point(error_point)

    assert incident is not None
    assert incident.trigger_metric == "error_rate_percent"
    assert incident.trigger_value == 40.0


# ==========================================
# 3. Autonomous Self-Healing Controller Tests
# ==========================================

def test_self_healer_auto_scaling():
    async def _test():
        healer = SelfHealingController()
        service = "order-api"
        initial_replicas = healer.service_states[service].replicas

        incident = CloudIncident(
            title="High Latency on order-api",
            service_id=service,
            severity="HIGH",
            root_cause="Spike in checkout demand",
            trigger_metric="latency_p95_ms",
            trigger_value=220.0,
        )

        await healer.handle_incident_event("incident.detected", incident)

        # Replica count should have scaled up
        scaled_replicas = healer.service_states[service].replicas
        assert scaled_replicas > initial_replicas
        assert len(incident.remediation_actions) == 1
        assert incident.remediation_actions[0].action_type == "AUTO_SCALE"

    asyncio.run(_test())


def test_self_healer_circuit_breaker_tripping():
    async def _test():
        healer = SelfHealingController()
        service = "database-cluster"

        incident = CloudIncident(
            title="Cascading 5xx errors on database-cluster",
            service_id=service,
            severity="CRITICAL",
            root_cause="HTTP 5xx Storm localized to cluster",
            trigger_metric="error_rate_percent",
            trigger_value=35.0,
        )

        await healer.handle_incident_event("incident.detected", incident)

        node = healer.service_states[service]
        assert node.circuit_breaker_open is True
        assert node.rate_limit_active is True
        assert incident.remediation_actions[0].action_type == "CIRCUIT_BREAKER_TRIP"

    asyncio.run(_test())


# ==========================================
# 4. API Gateway, Health Probes & Metrics Tests
# ==========================================

@pytest.fixture
def client():
    from backend.main import app
    with TestClient(app) as c:
        yield c


def test_health_probes(client):
    res = client.get("/healthz")
    assert res.status_code == 200
    assert res.json()["status"] == "HEALTHY"

    res_live = client.get("/livez")
    assert res_live.status_code == 200

    res_ready = client.get("/readyz")
    assert res_ready.status_code == 200
    assert res_ready.json()["status"] == "READY"


def test_prometheus_metrics_format(client):
    res = client.get("/metrics")
    assert res.status_code == 200
    text = res.text
    assert "# HELP cloudpulse_events_published_total" in text
    assert "# TYPE cloudpulse_events_published_total counter" in text
    assert "cloudpulse_dlq_size" in text
    assert "cloudpulse_service_replicas" in text


def test_telemetry_ingestion_api(client):
    payload = {
        "service_id": "api-gateway",
        "region": "us-east-1",
        "request_count": 120,
        "error_count": 0,
        "latency_p95_ms": 18.5,
        "cpu_percent": 28.0,
        "memory_percent": 42.0,
    }
    res = client.post("/api/telemetry", json=payload)
    assert res.status_code == 200
    data = res.json()
    assert data["status"] == "INGESTED"
    assert "trace_id" in data


def test_topology_and_chaos_api(client):
    res = client.get("/api/topology")
    assert res.status_code == 200
    topology = res.json()
    assert len(topology) == 6

    # Inject chaos
    chaos_req = {
        "target_service": "payment-gateway",
        "fault_type": "LATENCY_SPIKE",
        "duration_seconds": 10,
        "intensity": 1.5,
    }
    res_chaos = client.post("/api/chaos/inject", json=chaos_req)
    assert res_chaos.status_code == 200
    assert res_chaos.json()["status"] == "INJECTED"

    # Clear chaos
    res_clear = client.post("/api/chaos/clear")
    assert res_clear.status_code == 200
    assert res_clear.json()["status"] == "CLEARED"
