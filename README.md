# CloudPulse: Cloud-Native Autonomous Telemetry & Self-Healing SRE Platform

![CloudPulse Architecture](https://img.shields.io/badge/Architecture-Cloud--Native%20EDA-06b6d4?style=for-the-badge)
![Python](https://img.shields.io/badge/Python-3.9+-3776ab?style=for-the-badge&logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-0.110+-009688?style=for-the-badge&logo=fastapi&logoColor=white)
![Terraform](https://img.shields.io/badge/IaC-Terraform%20AWS-7b42bc?style=for-the-badge&logo=terraform&logoColor=white)
![Kubernetes](https://img.shields.io/badge/Orchestration-Kubernetes%20HPA-326ce5?style=for-the-badge&logo=kubernetes&logoColor=white)

---

## 1. Real-World Problem Statement

In enterprise cloud computing (AWS, GCP, Azure), microservice architectures suffer from:
1. **Cascading Failure Cascades**: When a shared downstream component (like a relational database cluster) experiences connection pool exhaustion, dependent microservices (API Gateways, Checkout services, Payment processors) experience sudden latency degradation and HTTP 5xx storms.
2. **Alert Storms & Operator Fatigue**: A single root failure routinely fires 50+ alerts across different PagerDuty teams, overwhelming on-call SRE engineers and delaying root-cause identification.
3. **Slow MTTR (Mean Time To Recovery)**: Enterprise cloud downtime costs an estimated **$300,000+ per hour**. Manual human diagnosis and intervention cannot keep pace with distributed system failures.

### The Solution: CloudPulse
**CloudPulse** is an enterprise-grade, event-driven cloud observability and autonomous resilience platform. It ingests distributed telemetry streams, applies real-time statistical anomaly detection (dynamic Z-scores & EWMA), correlates multi-service blast radiuses to identify true root causes, and executes closed-loop self-healing policies (horizontal cloud autoscaling, circuit breaker tripping, container restarts, and traffic shedding) before customer SLAs are breached.

---

## 2. Architecture & Design

```
+----------------------------------------------------------------------------------------------------+
|                                      CLOUDPULSE PLATFORM                                            |
+----------------------------------------------------------------------------------------------------+
|                                                                                                    |
|   [ Microservices Mesh: API Gateway, Auth, Order API, Payment, DB Cluster, Inventory ]             |
|                                         |                                                          |
|                                         | Telemetry (Latency, CPU, Error Rates, Traces)            |
|                                         v                                                          |
|   +--------------------------------------------------------------------------------------------+   |
|   | 1. Cloud Ingestion Gateway (FastAPI + Pydantic Schema Validation + Distributed Tracing)     |   |
|   +--------------------------------------------------------------------------------------------+   |
|                                         | Publishes `telemetry.raw`                                |
|                                         v                                                          |
|   +--------------------------------------------------------------------------------------------+   |
|   | 2. Event-Driven Message Bus (Asynchronous Pub/Sub + Priority Queues + Dead Letter Queue)   |   |
|   +--------------------------------------------------------------------------------------------+   |
|             |                                                  |                                   |
|             v                                                  v                                   |
|   +-----------------------------------+              +-----------------------------------------+   |
|   | 3. Streaming Anomaly Engine       |              | 4. Autonomous Self-Healing Controller   |   |
|   | - Dynamic Z-Score / EWMA Latency  |              | - Horizontal Cloud Autoscaling (HPA)    |   |
|   | - Dynamic Error Rate Thresholds   |              | - Circuit Breaker Tripping (50% shed)   |   |
|   | - Topology Blast Radius Analysis  |              | - Container Restarts & Cache Evacuation |   |
|   +-----------------------------------+              +-----------------------------------------+   |
|                      \                                            /                                |
|                       v                                          v                                 |
|   +--------------------------------------------------------------------------------------------+   |
|   | 5. Observability & Open Telemetry Gateway                                                  |   |
|   | - Prometheus `/metrics` exposition (OpenMetrics compliant)                                 |   |
|   | - Cloud Health Probes (`/healthz`, `/livez`, `/readyz`)                                    |   |
|   | - Server-Sent Events (SSE) `/api/stream` real-time push                                    |   |
|   +--------------------------------------------------------------------------------------------+   |
|                                         ^                                                          |
|                                         | Real-Time Event Stream (SSE)                             |
|   +--------------------------------------------------------------------------------------------+   |
|   | 6. SRE Command Center (Glassmorphic Web Dashboard)                                         |   |
|   | - Microservice Topology Grid & Status Badges                                               |   |
|   | - Live KPI Telemetry (Bus Events, DLQ count, Replicas, Incident Radar)                     |   |
|   | - Interactive Chaos Engineering Console (Inject DB Saturation, Latency Spikes, 500 Storms) |   |
|   +--------------------------------------------------------------------------------------------+   |
+----------------------------------------------------------------------------------------------------+
```

---

## 3. Project Structure

```
cloudpulse/
├── backend/
│   ├── config.py             # System thresholds, SLA limits, and environment settings
│   ├── models.py             # Pydantic models for Telemetry, Incidents, and Self-Healing
│   ├── event_bus.py          # Asynchronous Pub/Sub engine with Dead Letter Queue (DLQ)
│   ├── anomaly_detector.py   # Statistical streaming Z-Score & blast-radius correlator
│   ├── self_healer.py        # Autonomous cloud remediation controller
│   ├── simulator.py          # Multi-service telemetry traffic & chaos fault injector
│   └── main.py               # FastAPI application, Prometheus /metrics & SSE stream
├── frontend/
│   ├── index.html            # Dark-mode glassmorphic SRE Command Center
│   ├── styles.css            # Responsive styles, glowing status badges, animations
│   └── app.js                # SSE client, real-time DOM hydration, chaos buttons
├── deploy/
│   ├── Dockerfile            # Production multi-stage, non-root container image
│   ├── terraform/
│   │   └── main.tf           # AWS ECS Fargate, ALB, and SQS DLQ infrastructure
│   └── k8s/
│       └── kubernetes.yaml   # K8s Deployment, Service, ConfigMap, and HPA
├── tests/
│   └── test_cloudpulse.py    # Automated test suite (Pub/Sub, Anomaly Engine, Self-Healer)
├── requirements.txt          # Python dependencies
├── run.py                    # One-click execution launcher
└── README.md                 # Complete documentation
```

---

## 4. Quickstart & Execution

### 1. Run Automated Test Suite
Verify that all unit and integration tests pass:
```bash
python -m pytest tests/ -v
```

### 2. Launch the Platform
Start the CloudPulse server and open the browser dashboard:
```bash
python run.py
```
* The platform will start listening on `http://localhost:8000`.
* Your browser will automatically open to the **CloudPulse SRE Command Center**.

---

## 5. Testing Self-Healing with Chaos Engineering

Inside the dashboard (or via REST API), click any of the Chaos Fault buttons:

1. **💥 Exhaust DB Connection Pool**:
   - Injects database connection pool saturation on `database-cluster`.
   - Dependent services (`order-api`, `payment-gateway`) automatically experience cascading latency.
   - **Root Cause Correlator** flags the incident as cascading from `database-cluster`.
   - **Self-Healing Controller** trips the circuit breaker on `database-cluster`, sheds non-critical traffic, and restores system health.

2. **⏱️ Inject Latency Spike on Payment Gateway**:
   - P95 latency spikes to 280ms+ (Z-score > 3.0).
   - **Self-Healing Controller** detects the SLA violation and triggers **Horizontal Cloud Autoscaling (HPA)**, scaling replicas from 2 to 4.
   - Latency normalizes back to baseline.

3. **⚡ Inject HTTP 500 Storm on Order API**:
   - Error rate jumps to 35%+.
   - **Self-Healing Controller** opens circuit breaker, shedding traffic to prevent cascading failures.

---

## 6. Observability Endpoints

| Endpoint | Method | Description |
| :--- | :--- | :--- |
| `http://localhost:8000/` | `GET` | SRE Cloud Command Center UI |
| `http://localhost:8000/healthz` | `GET` | Kubernetes / Cloud ALB Liveness Probe |
| `http://localhost:8000/readyz` | `GET` | Kubernetes Readiness Probe |
| `http://localhost:8000/metrics` | `GET` | Prometheus / OpenMetrics Exposition |
| `http://localhost:8000/api/stream` | `GET` | Real-time Server-Sent Events (SSE) stream |
| `http://localhost:8000/api/topology`| `GET` | Current Microservice Mesh State |
| `http://localhost:8000/api/incidents`| `GET` | Active & historical incident log |
| `http://localhost:8000/api/chaos/inject` | `POST` | Trigger cloud fault injection |
| `http://localhost:8000/docs` | `GET` | Interactive Swagger / OpenAPI Specification |

---

## 7. Production Cloud Deployment

### AWS Deployment (Terraform)
The repository includes production Terraform configuration (`deploy/terraform/main.tf`):
```bash
cd deploy/terraform
terraform init
terraform plan
terraform apply
```
Provisions:
- Multi-AZ VPC with Public & Private subnets.
- Application Load Balancer with HTTP/HTTPS listeners and health probes.
- AWS SQS Ingestion Queue with Dead Letter Queue (DLQ) (14-day retention).
- AWS ECS Fargate cluster with automated task scheduling.

### Kubernetes Deployment
```bash
kubectl apply -f deploy/k8s/kubernetes.yaml
```
Deploys:
- Zero-downtime RollingUpdate Deployment with CIS-compliant non-root containers.
- Liveness and Readiness probes configured to `/healthz` and `/readyz`.
- Horizontal Pod Autoscaler (HPA) scaling pods dynamically from 2 to 10 instances.
