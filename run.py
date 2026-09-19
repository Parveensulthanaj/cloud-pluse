"""
CloudPulse Launcher & SRE Operational Console.
Executes the CloudPulse platform, starts the background cloud simulator,
and launches the browser-based SRE dashboard.
"""
import os
import sys
import threading
import time
import webbrowser
import uvicorn

BANNER = r"""
  ____ _                 _ ____        _          
 / ___| | ___  _   _  __| |  _ \ _   _| |___  ___ 
| |   | |/ _ \| | | |/ _` | |_) | | | | / __|/ _ \
| |___| | (_) | |_| | (_| |  __/| |_| | \__ \  __/
 \____|_|\___/ \__,_|\__,_|_|    \__,_|_|___/\___|
  Autonomous Cloud Telemetry & Self-Healing SRE Mesh
"""


def open_browser():
    time.sleep(1.5)
    url = "http://localhost:8000"
    print(f"\n[+] Opening CloudPulse SRE Command Center: {url}\n")
    try:
        webbrowser.open(url)
    except Exception as e:
        print(f"[-] Could not auto-open browser: {e}")


def main():
    print(BANNER)
    print("=" * 65)
    print(" [*] Architecture: Distributed Cloud Microservices + EventBus + DLQ")
    print(" [*] Anomaly Detection: Statistical Dynamic Z-Score & EWMA")
    print(" [*] Closed-Loop SRE: Horizontal Auto-Scaling & Circuit Breaking")
    print(" [*] Observability: Prometheus /metrics & Health Probes (/healthz)")
    print("=" * 65)
    print(" [>] Web Dashboard:      http://localhost:8000")
    print(" [>] Prometheus Metrics: http://localhost:8000/metrics")
    print(" [>] Health Liveness:    http://localhost:8000/healthz")
    print(" [>] OpenAPI Specs:      http://localhost:8000/docs")
    print("=" * 65)

    # Launch browser in a background daemon thread
    if not os.getenv("CLOUDPULSE_NO_BROWSER"):
        threading.Thread(target=open_browser, daemon=True).start()

    # Launch ASGI web server
    uvicorn.run(
        "backend.main:app",
        host="0.0.0.0",
        port=8000,
        log_level="info",
        reload=False,
    )


if __name__ == "__main__":
    main()
