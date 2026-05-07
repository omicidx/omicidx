import time
import uuid

from loguru import logger
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        request_id = str(uuid.uuid4())
        request.state.request_id = request_id

        start = time.monotonic()
        response = await call_next(request)
        duration_ms = int((time.monotonic() - start) * 1000)

        route = request.scope.get("route")
        endpoint = route.path if route else request.url.path

        logger.info(
            "request",
            request_id=request_id,
            method=request.method,
            path=endpoint,
            status_code=response.status_code,
            duration_ms=duration_ms,
            accession=request.path_params.get("accession", ""),
        )

        response.headers["X-Request-ID"] = request_id
        return response
