"""
CloudPulse Root Entrypoint for Cloud Deployments (Vercel, Render, Railway, AWS).
Exposes FastAPI 'app' in the root default location.
"""
import os
import sys

# Ensure current root directory is in sys.path
root_dir = os.path.dirname(os.path.abspath(__file__))
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

from backend.main import app

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
