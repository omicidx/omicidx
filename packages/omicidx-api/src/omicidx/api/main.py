import sys
from contextlib import asynccontextmanager

from fastapi import FastAPI
from loguru import logger
from omicidx.api.db import engine
from omicidx.api.middleware.logging import RequestLoggingMiddleware
from omicidx.api.routers import bioproject, biosample, geo, pubmed, sra

# Configure loguru: JSON to stdout for structured logging
logger.remove()
logger.add(sys.stdout, serialize=True, level="INFO")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("omicidx-api starting")
    yield
    await engine.dispose()
    logger.info("omicidx-api shutdown")


app = FastAPI(
    title="OmicIDX API",
    version="0.1.0",
    description="Read-only REST API for OmicIDX entity lookups",
    lifespan=lifespan,
)

app.add_middleware(RequestLoggingMiddleware)

app.include_router(bioproject.router, prefix="/v1/bioproject", tags=["bioproject"])
app.include_router(biosample.router, prefix="/v1/biosample", tags=["biosample"])
app.include_router(sra.router, prefix="/v1/sra", tags=["sra"])
app.include_router(geo.router, prefix="/v1/geo", tags=["geo"])
app.include_router(pubmed.router, prefix="/v1/pubmed", tags=["pubmed"])


@app.get("/v1/health")
async def health():
    return {"status": "ok"}


@app.get("/v1/")
async def root():
    return {
        "name": "OmicIDX API",
        "version": "0.1.0",
        "entities": [
            "bioproject",
            "biosample",
            "sra/studies",
            "sra/samples",
            "sra/experiments",
            "sra/runs",
            "geo/series",
            "geo/samples",
            "geo/platforms",
            "pubmed",
        ],
    }
