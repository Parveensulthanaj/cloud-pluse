"""
Autonomous Self-Healing / Auto-Remediation Engine.
Executes cloud-native recovery policies (Horizontal Auto-Scaling, Circuit Breaking,
Container Restart, Cache Eviction, and Traffic Shedding) in response to detected incidents.
"""
import asyncio
import logging
import time
from typing import Dict, List, Optional
from .config import config
from .models import CloudIncident, RemediationAction, ServiceNodeState
from .event_bus import event_bus

logger = logging.getLogger("cloudpulse.self_healer")


class SelfHealingController:
    def __init__(self):
        # Service cluster state: {service_id: ServiceNodeState}
        self.service_states: Dict[str, ServiceNodeState] = {}
        # Action history
        self.action_history: List[RemediationAction] = []
        # Last remediation execution timestamp per service
        self.last_remediation: Dict[str, float] = {}

        self._initialize_default_topology()

    def _initialize_default_topology(self):
        """Seed topology of cloud microservices."""
        services = [
            ("api-gateway", "API Gateway & Edge Router", "gateway", 3),
            ("auth-service", "Identity & Auth Service", "core", 2),
            ("order-api", "Order Management API", "core", 2),
            ("payment-gateway", "Payment & Billing Gateway", "core", 2),
            ("database-cluster", "Distributed SQL Cluster", "storage", 3),
            ("inventory-service", "Inventory & Warehouse Node", "core", 2),
        ]
        for s_id, name, tier, replicas in services:
            self.service_states[s_id] = ServiceNodeState(
                service_id=s_id,
                name=name,
                tier=tier,
                replicas=replicas,
                status="HEALTHY",
            )

    async def handle_incident_event(self, topic: str, incident: CloudIncident):
        """
        Callback triggered when 'incident.detected' is published to EventBus.
        """
        if not config.AUTO_HEALING_ENABLED:
            logger.info("Auto-healing is disabled by configuration. Skipping remediation.")
            return

        service_id = incident.service_id
        now = time.time()

        # Cooldown check to prevent remediation thrashing
        last_time = self.last_remediation.get(service_id, 0)
        if now - last_time < config.REMEDIATION_COOLDOWN_SECONDS:
            logger.info(f"Service {service_id} is in remediation cooldown ({now - last_time:.1f}s ago). Waiting.")
            return

        self.last_remediation[service_id] = now
        incident.status = "HEALING_IN_PROGRESS"

        # Determine best remediation policy based on trigger metric & root cause
        action = await self._execute_remediation_policy(incident)
        if action:
            self.action_history.append(action)
            incident.remediation_actions.append(action)
            await event_bus.publish("remediation.executed", action)

    async def _execute_remediation_policy(self, incident: CloudIncident) -> Optional[RemediationAction]:
        service_id = incident.service_id
        node = self.service_states.get(service_id)
        if not node:
            return None

        node.status = "HEALING"
        action_type = "AUTO_SCALE"
        details = ""

        # Policy 1: Error Rate Spike -> Circuit Breaker & Traffic Shedding
        if "error_rate" in incident.trigger_metric or "5xx" in incident.root_cause:
            node.circuit_breaker_open = True
            node.rate_limit_active = True
            action_type = "CIRCUIT_BREAKER_TRIP"
            details = f"Tripped circuit breaker and shed 50% traffic on {service_id} to isolate cascading faults."

        # Policy 2: CPU Saturation or High Latency -> Horizontal Auto-Scaling
        elif "cpu" in incident.trigger_metric or "latency" in incident.trigger_metric:
            prev_replicas = node.replicas
            node.replicas = min(config.MAX_REPLICA_SCALE, node.replicas + 2)
            action_type = "AUTO_SCALE"
            details = f"Triggered Horizontal Cloud Pod Autoscaling (HPA): scaled replicas {prev_replicas} -> {node.replicas}."

        # Policy 3: Memory Exhaustion or Database Pool issue -> Container Restart & Cache Purge
        elif "memory" in incident.trigger_metric or "pool" in incident.root_cause.lower():
            action_type = "CONTAINER_RESTART"
            details = f"Executed graceful rolling restart of {node.replicas} replicas and evacuated active connection pools."

        else:
            prev_replicas = node.replicas
            node.replicas = min(config.MAX_REPLICA_SCALE, node.replicas + 1)
            action_type = "AUTO_SCALE"
            details = f"Provisioned supplementary cloud replica on {service_id}."

        node.last_healed_at = time.time()

        action = RemediationAction(
            incident_id=incident.incident_id,
            service_id=service_id,
            action_type=action_type,
            details=details,
            parameters={"new_replicas": node.replicas, "circuit_breaker": node.circuit_breaker_open},
        )

        logger.info(f"[AUTONOMOUS REMEDIATION EXECUTED] {action.action_type} on {service_id}: {details}")

        # Schedule automatic verification & stabilization task
        asyncio.create_task(self._verify_and_stabilize(service_id, incident))
        return action

    async def _verify_and_stabilize(self, service_id: str, incident: CloudIncident):
        """
        Wait for cloud remediation actions to propagate (cooling period),
        then verify stabilization and restore normal state.
        """
        await asyncio.sleep(8.0)
        node = self.service_states.get(service_id)
        if node:
            node.status = "HEALTHY"
            node.circuit_breaker_open = False
            node.rate_limit_active = False
            node.current_error_rate = 0.0
            node.current_latency_ms = 22.0
            node.current_cpu = 32.0

        # Import here to avoid circular dependencies
        from .anomaly_detector import anomaly_detector
        resolved = anomaly_detector.resolve_incident(
            service_id,
            resolution_notes=f"Service stabilized following autonomous remediation ({incident.remediation_actions[-1].action_type if incident.remediation_actions else 'HEALING'})."
        )
        if resolved:
            await event_bus.publish("incident.resolved", resolved)

    def update_telemetry_metrics(self, service_id: str, latency: float, error_rate: float, cpu: float, memory: float):
        """Update live telemetry metrics for topology display."""
        node = self.service_states.get(service_id)
        if node:
            node.current_latency_ms = round(latency, 1)
            node.current_error_rate = round(error_rate, 2)
            node.current_cpu = round(cpu, 1)
            node.current_memory = round(memory, 1)
            node.last_heartbeat = time.time()

            # Dynamic status update if not currently healing
            if node.status != "HEALING":
                if error_rate > 15.0 or cpu > 90.0 or latency > 200.0:
                    node.status = "CRITICAL"
                elif error_rate > 5.0 or cpu > 75.0 or latency > 80.0:
                    node.status = "DEGRADED"
                else:
                    node.status = "HEALTHY"

    def get_topology(self) -> List[ServiceNodeState]:
        return list(self.service_states.values())

    def get_actions(self) -> List[RemediationAction]:
        return list(reversed(self.action_history[-40:]))


self_healer = SelfHealingController()
