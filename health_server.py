# health_server.py

from fastapi import FastAPI
import uvicorn
import threading

app = FastAPI()

@app.get("/health")
def health():
    return {"status": "ok"}

def start_health_server():
    thread = threading.Thread(
        target=lambda: uvicorn.run(app, host="0.0.0.0", port=8001, log_level="warning"),
        daemon=True
    )
    thread.start()