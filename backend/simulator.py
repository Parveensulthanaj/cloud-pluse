"""
Cloud Telemetry Simulator & Chaos Engineering Injector.
Generates realistic multi-region cloud microservice traffic and allows
on-demand injection of real-world cloud failure modes (Latency Spikes,
Database Saturation, Cascading 5xx Storms, Memory Leaks).
"""
import asyncio
import random
import time
import logging
from typing import Dict, Optional
from .models import ChaosInjectionRequest, TelemetryPoint
from .event_bus import event_bus
from .anomaly_detector import anomaly_detector
from .self_healer import self_healer

logger = logging.getLogger("cloudpulse.simulator")


class CloudSimulator:
    SERVICES = [
        ("api-gateway", "us-east-1", 18.0, 30.0),
        ("auth-service", "us-east-1", 12.0, 25.0),
        ("order-api", "us-east-1", 28.0, 35.0),
        ("payment-gateway", "us-east-1", 35.0, 40.0),
        ("database-cluster", "us-east-1", 15.0, 45.0),
        ("inventory-service", "us-east-1", 22.0, 28.0),
    ]

    def __init__(self):
        self._running = False
        self._task: Optional[asyncio.Task] = None
        # Active chaos injections: {service_id: {"fault_type": str, "expires_at": float, "intensity": float}}
        self.active_chaos: Dict[str, dict] = {}

    def start(self):
        if not self._running:
            self._running = True
            self._task = asyncio.create_task(self._simulation_loop())
            logger.info("Cloud Telemetry Simulator started.")

    def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            logger.info("Cloud Telemetry Simulator stopped.")

    def inject_chaos(self, request: ChaosInjectionRequest) -> dict:
        """Inject a chaos engineering fault into a target microservice."""
        expires_at = time.time() + request.duration_seconds
        self.active_chaos[request.target_service] = {
            "fault_type": request.fault_type,
            "expires_at": expires_at,
            "intensity": request.intensity,
        }
        logger.warning(
            f"[CHAOS INJECTED] Target: {request.target_service}, Fault: {request.fault_type}, "
            f"Duration: {request.duration_seconds}s, Intensity: {request.intensity}"
        )
        return {
            "status": "INJECTED",
            "target": request.target_service,
            "fault": request.fault_type,
            "duration": request.duration_seconds,
            "expires_at": expires_at,
        }

    def clear_chaos(self, service_id: Optional[str] = None):
        """Clear active chaos injections."""
        if service_id:
            self.active_chaos.pop(service_id, None)
        else:
            self.active_chaos.clear()

    async def _simulation_loop(self):
        while self._running:
            try:
                now = time.time()
                # Clean up expired chaos faults
                expired = [s for s, c in self.active_chaos.items() if now > c["expires_at"]]
                for s in expired:
                    del self.active_chaos[s]
                    logger.info(f"Chaos fault expired for service: {s}")

                # Generate telemetry for each microservice
                for s_id, region, base_lat, base_cpu in self.SERVICES:
                    point = self._generate_point(s_id, region, base_lat, base_cpu, now)

                    # Update node state in self-healer
                    self_healer.update_telemetry_metrics(
                        service_id=s_id,
                        latency=point.latency_p95_ms,
                        error_rate=point.error_rate_percent,
                        cpu=point.cpu_percent,
                        memory=point.memory_percent,
                    )

                    # Publish to EventBus
                    await event_bus.publish("telemetry.raw", point)

                    # Ingest into Anomaly Detector
                    incident = anomaly_detector.ingest_point(point)
                    if incident:
                        await event_bus.publish("incident.detected", incident)

                await asyncio.sleep(1.8)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in simulation loop: {e}", exc_info=True)
                await asyncio.sleep(2.0)

    def _generate_point(self, service_id: str, region: str, base_lat: float, base_cpu: float, now: float) -> TelemetryPoint:
        node = self_healer.service_states.get(service_id)
        is_healing = node.status == "HEALING" if node else False
        is_cb_open = node.circuit_breaker_open if node else False
        replicas = node.replicas if node else 2

        # Check if service is directly under chaos or impacted by upstream failure
        chaos = self.active_chaos.get(service_id)
        # Check if upstream database is failing
        upstream_failing = (
            service_id in ["order-api", "payment-gateway", "inventory-service"]
            and "database-cluster" in self.active_chaos
        )

        req_count = random.randint(80, 160)
        error_count = 0
        lat = base_lat + random.uniform(-2.0, 4.0)
        cpu = base_cpu + random.uniform(-3.0, 5.0)
        mem = 40.0 + random.uniform(-2.0, 4.0)

        # Baseline jitter
        if random.random() < 0.05:
            error_count = random.randint(1, 2)

        # Apply chaos effects
        if chaos:
            fault = chaos["fault_type"]
            mult = chaos.get("intensity", 1.0)

            if fault == "LATENCY_SPIKE":
                lat = 280.0 * mult + random.uniform(-10.0, 30.0)
                cpu += 20.0 * mult
            elif fault == "DB_POOL_EXHAUSTION":
                lat = 340.0 * mult + random.uniform(0.0, 50.0)
                cpu = min(98.0, 88.0 * mult + random.uniform(0.0, 8.0))
                error_count = int(req_count * min(0.45, 0.28 * mult))
            elif fault == "HTTP_500_STORM":
                error_count = int(req_count * min(0.65, 0.40 * mult))
                lat += 40.0
            elif fault == "MEMORY_LEAK":
                mem = min(99.0, 89.0 + random.uniform(2.0, 9.0))
                cpu += 30.0

        elif upstream_failing:
            # Cascading degradation from upstream
            lat += 120.0 + random.uniform(10.0, 40.0)
            error_count = int(req_count * 0.15)
            cpu += 25.0

        # Self-healing mitigation impact:
        # If replicas scaled up, latency & CPU drop back towards normal!
        if replicas > 2:
            scale_benefit = 1.0 - (min(replicas - 2, 4) * 0.15)
            lat = max(base_lat, lat * scale_benefit)
            cpu = max(base_cpu, cpu * scale_benefit)

        # If Circuit Breaker is open, traffic is shed, reducing error rate to zero
        if is_cb_open:
            req_count = int(req_count * 0.4)  # Shed non-critical traffic
            error_count = 0                   # Circuit breaker prevents 500 downstream errors
            lat = base_lat * 1.1

        return TelemetryPoint(
            service_id=service_id,
            region=region,
            timestamp=now,
            request_count=max(10, req_count),
            error_count=max(0, error_count),
            latency_p95_ms=round(max(5.0, lat), 2),
            cpu_percent=round(min(99.9, max(5.0, cpu)), 2),
            memory_percent=round(min(99.9, max(10.0, mem)), 2),
        )


simulator = CloudSimulator()
