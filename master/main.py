"""
master/main.py  —  Master Server entry point
Person A: run with:
  cd f:/dormpool/dormpool
  DORMPOOL_TOKEN=yourtoken uvicorn master.main:app --host 0.0.0.0 --port 8000
"""
from master.app.m1_api import app  # noqa: F401 — uvicorn targets this

# Serve Team Console static file at /console
from fastapi.responses import FileResponse
from fastapi import Request

@app.get("/console")
def console(request: Request):
    return FileResponse("console/index.html")
