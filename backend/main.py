"""
CloudPulse Main API Gateway & Service Controller.
Provides REST APIs, Server-Sent Events (SSE) telemetry stream, Prometheus `/metrics`,
Cloud Health Probes, and serves the SRE Command Center dashboard.
"""
import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from .config import config
from .models import (
    ChaosInjectionRequest,
    CloudIncident,
    TelemetryBatch,
    TelemetryPoint,
)
from .event_bus import event_bus
from .anomaly_detector import anomaly_detector
from .self_healer import self_healer
from .simulator import simulator

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("cloudpulse.main")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Setup subscribers
    event_bus.subscribe("incident.detected", self_healer.handle_incident_event)
    # Start traffic simulation
    simulator.start()
    logger.info(f"{config.APP_NAME} v{config.APP_VERSION} initialized and running.")
    yield
    # Graceful shutdown
    simulator.stop()
    logger.info("CloudPulse shutting down gracefully.")


app = FastAPI(
    title=config.APP_NAME,
    version=config.APP_VERSION,
    description="Enterprise Cloud-Native Telemetry Ingestion, Anomaly Detection & Autonomous Self-Healing Platform",
    lifespan=lifespan,
)

# Enable CORS for cloud clients
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Distributed Tracing & Correlation Middleware
@app.middleware("http")
async def correlation_middleware(request: Request, call_next):
    corr_id = request.headers.get("X-Correlation-ID", f"req-{os.urandom(4).hex()}")
    response = await call_next(request)
    response.headers["X-Correlation-ID"] = corr_id
    return response


# ==========================================
# Cloud Health Probes & Prometheus Metrics
# ==========================================

@app.get("/healthz", tags=["Observability"])
@app.get("/livez", tags=["Observability"])
async def liveness_probe():
    """Kubernetes / Cloud Load Balancer liveness probe."""
    return {"status": "HEALTHY", "app": config.APP_NAME, "version": config.APP_VERSION}


@app.get("/readyz", tags=["Observability"])
async def readiness_probe():
    """Kubernetes readiness probe."""
    return {
        "status": "READY",
        "event_bus": "CONNECTED",
        "active_nodes": len(self_healer.service_states),
    }


@app.get("/metrics", tags=["Observability"])
async def prometheus_metrics():
    """Expose Prometheus-standard metrics for scraping."""
    stats = event_bus.get_stats()
    nodes = self_healer.get_topology()
    active_incidents = len(anomaly_detector.get_active_incidents())

    lines = [
        "# HELP cloudpulse_events_published_total Total telemetry events published to the message bus",
        "# TYPE cloudpulse_events_published_total counter",
        f"cloudpulse_events_published_total {stats['metrics']['published']}",
        "# HELP cloudpulse_events_delivered_total Total events successfully delivered to consumers",
        "# TYPE cloudpulse_events_delivered_total counter",
        f"cloudpulse_events_delivered_total {stats['metrics']['delivered']}",
        "# HELP cloudpulse_dlq_size Number of items currently stored in Dead Letter Queue",
        "# TYPE cloudpulse_dlq_size gauge",
        f"cloudpulse_dlq_size {stats['dlq_size']}",
        "# HELP cloudpulse_active_incidents Current count of open anomalies and incidents",
        "# TYPE cloudpulse_active_incidents gauge",
        f"cloudpulse_active_incidents {active_incidents}",
    ]

    for node in nodes:
        labels = f'service="{node.service_id}",tier="{node.tier}"'
        lines.append(f"cloudpulse_service_replicas{{{labels}}} {node.replicas}")
        lines.append(f"cloudpulse_service_latency_ms{{{labels}}} {node.current_latency_ms}")
        lines.append(f"cloudpulse_service_error_rate_percent{{{labels}}} {node.current_error_rate}")
        lines.append(f"cloudpulse_service_cpu_percent{{{labels}}} {node.current_cpu}")
        lines.append(f"cloudpulse_service_circuit_breaker{{{labels}}} {1 if node.circuit_breaker_open else 0}")

    return Response(content="\n".join(lines) + "\n", media_type="text/plain; version=0.0.4")


# ==========================================
# Ingestion Gateway & Telemetry APIs
# ==========================================

@app.post("/api/telemetry", tags=["Ingestion"])
async def ingest_telemetry(point: TelemetryPoint):
    """
    Ingest a single telemetry point from an edge gateway or external microservice.
    """
    self_healer.update_telemetry_metrics(
        service_id=point.service_id,
        latency=point.latency_p95_ms,
        error_rate=point.error_rate_percent,
        cpu=point.cpu_percent,
        memory=point.memory_percent,
    )
    await event_bus.publish("telemetry.raw", point)
    incident = anomaly_detector.ingest_point(point)
    if incident:
        await event_bus.publish("incident.detected", incident)

    return {"status": "INGESTED", "trace_id": point.trace_id, "anomaly_flagged": incident is not None}


@app.post("/api/telemetry/batch", tags=["Ingestion"])
async def ingest_telemetry_batch(batch: TelemetryBatch):
    """Ingest a batch of cloud telemetry points."""
    anomalies = 0
    for point in batch.points:
        self_healer.update_telemetry_metrics(
            service_id=point.service_id,
            latency=point.latency_p95_ms,
            error_rate=point.error_rate_percent,
            cpu=point.cpu_percent,
            memory=point.memory_percent,
        )
        await event_bus.publish("telemetry.raw", point)
        incident = anomaly_detector.ingest_point(point)
        if incident:
            anomalies += 1
            await event_bus.publish("incident.detected", incident)

    return {"status": "BATCH_PROCESSED", "batch_id": batch.batch_id, "points": len(batch.points), "anomalies": anomalies}


# ==========================================
# SRE Topology, Incidents & Self-Healing APIs
# ==========================================

@app.get("/api/topology", tags=["SRE Management"])
async def get_service_topology():
    """Return microservice mesh topology and live node states."""
    return self_healer.get_topology()


@app.get("/api/incidents", tags=["SRE Management"])
async def get_incidents():
    """Return active and recent cloud incidents."""
    return {
        "active": [inc.dict() for inc in anomaly_detector.get_active_incidents()],
        "history": [inc.dict() for inc in anomaly_detector.get_all_incidents()],
    }


@app.get("/api/actions", tags=["SRE Management"])
async def get_remediation_actions():
    """Return chronological log of autonomous cloud self-healing actions."""
    return [act.dict() for act in self_healer.get_actions()]


@app.get("/api/bus/stats", tags=["SRE Management"])
async def get_bus_stats():
    """Return EventBus metrics, recent messages, and DLQ contents."""
    return {
        **event_bus.get_stats(),
        "recent_events": event_bus.get_recent_events(),
        "dlq_events": event_bus.get_dlq_events(),
    }


# ==========================================
# Chaos Engineering Injection APIs
# ==========================================

@app.post("/api/chaos/inject", tags=["Chaos Engineering"])
async def inject_chaos(req: ChaosInjectionRequest):
    """Inject a cloud fault into a target service to observe self-healing."""
    valid_services = list(self_healer.service_states.keys())
    if req.target_service not in valid_services:
        raise HTTPException(status_code=400, detail=f"Target service '{req.target_service}' not found. Valid: {valid_services}")
    return simulator.inject_chaos(req)


@app.post("/api/chaos/clear", tags=["Chaos Engineering"])
async def clear_chaos():
    """Clear all active chaos injection faults."""
    simulator.clear_chaos()
    return {"status": "CLEARED"}


# ==========================================
# Real-Time SSE Stream for SRE Dashboard
# ==========================================

async def sse_event_generator() -> AsyncGenerator[str, None]:
    """Streams live telemetry, active incidents, and node status every 1.5s."""
    while True:
        try:
            snapshot = {
                "nodes": [n.dict() for n in self_healer.get_topology()],
                "active_incidents": [inc.dict() for inc in anomaly_detector.get_active_incidents()],
                "recent_actions": [act.dict() for act in self_healer.get_actions()[:5]],
                "bus_stats": event_bus.get_stats(),
                "chaos": simulator.active_chaos,
            }
            yield f"data: {json.dumps(snapshot)}\n\n"
            await asyncio.sleep(1.5)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"SSE generator error: {e}")
            await asyncio.sleep(2.0)


@app.get("/api/stream", tags=["Real-Time"])
async def stream_telemetry():
    """Server-Sent Events (SSE) endpoint providing 1.5-second live telemetry updates."""
    return StreamingResponse(
        sse_event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
    )


# ==========================================
# Static Frontend Dashboard Serving
# ==========================================

frontend_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "frontend"))
if os.path.exists(frontend_dir):
    app.mount("/static", StaticFiles(directory=frontend_dir), name="static")

    @app.get("/", response_class=HTMLResponse, tags=["Dashboard"])
    async def serve_index():
        index_path = os.path.join(frontend_dir, "index.html")
        if os.path.exists(index_path):
            with open(index_path, "r", encoding="utf-8") as f:
                return HTMLResponse(content=f.read())
        return HTMLResponse("<h1>CloudPulse Dashboard Initializing...</h1>")
