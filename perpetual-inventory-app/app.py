"""FreshMart Inventory Intelligence - FastAPI Application."""

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pathlib import Path

from server.routes import dashboard, anomalies, agent_route, validations, stores, analytics

app = FastAPI(title="FreshMart Inventory Intelligence")

# Register API routes
app.include_router(dashboard.router)
app.include_router(anomalies.router)
app.include_router(agent_route.router)
app.include_router(validations.router)
app.include_router(stores.router)
app.include_router(analytics.router)


@app.get("/api/health")
def health():
    return {"status": "ok", "app": "perpetual-inventory-engine"}


# Serve frontend static files
FRONTEND_DIR = Path(__file__).parent / "frontend" / "dist"

if FRONTEND_DIR.exists():
    app.mount("/assets", StaticFiles(directory=FRONTEND_DIR / "assets"), name="assets")

    @app.get("/{full_path:path}")
    def serve_spa(full_path: str):
        file_path = FRONTEND_DIR / full_path
        if file_path.exists() and file_path.is_file():
            return FileResponse(file_path)
        return FileResponse(FRONTEND_DIR / "index.html")
