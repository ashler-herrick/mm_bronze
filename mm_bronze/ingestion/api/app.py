"""
Entrypoint for the FastAPI application using lifespan event handlers.
- Instantiates FastAPI
- Includes API routers
- Manages startup/shutdown via lifespan context
"""

from fastapi import FastAPI
from contextlib import asynccontextmanager

from mm_bronze.common.kafka import init_async_producer, close_async_producer
from mm_bronze.common.log_config import configure_logging
from mm_bronze.ingestion.api.api import router as ingestion_router


configure_logging()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: initialize Kafka producer
    await init_async_producer()
    try:
        yield
    finally:
        # Shutdown: close Kafka producer gracefully
        await close_async_producer()


def create_app() -> FastAPI:
    app = FastAPI(
        title="Ingestion Service",
        version="0.1.0",
        docs_url="/docs",
        redoc_url="/redoc",
        lifespan=lifespan,
    )

    # Include API routers
    app.include_router(ingestion_router)

    return app


# instantiate the ASGI app
app = create_app()
