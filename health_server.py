# health_server.py
# creates a webserver to monitor pipeline uptime

from fastapi import FastAPI # web framework to create API endpoints
import uvicorn # web server to run FastAPI
import threading

app = FastAPI()

# this is the health heartbeat
@app.get("/health")
def health():
    return {"status": "ok"}

def start_health_server():
    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="warning")