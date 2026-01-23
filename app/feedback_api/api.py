import os
import time
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Request, Response
from .models import FeedbackPayload, FeedbackResponse, ErrorResponse
from .rate_limit import RateLimiter
from .validation import validate_payload
from .storage import DuplicatePayloadError, write_feedback, payload_hash
from .storage import tracking_id_recent_exists
from .linking import link_run
from .linking import get_trace_timestamp
from .observability import (
    CORRELATION_HEADER,
    attach_correlation_id,
    get_or_create_correlation_id,
    log_event,
    metrics,
    record_latency,
)
from .policy import compile_regex, get_int_env_optional, get_policy


def create_app() -> FastAPI:
    app = FastAPI(title="Feedback API", version="v1")
    limiter = RateLimiter()

    @app.on_event("startup")
    def validate_startup() -> None:
        required = ["FEEDBACK_TABLE", "DATABRICKS_WAREHOUSE_ID"]
        missing = [name for name in required if not os.environ.get(name)]
        if missing:
            raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")
        compile_regex("TRACE_ID_REGEX")
        compile_regex("TRACKING_ID_REGEX")
        log_event(
            {
                "event": "startup_config",
                "has_trace_table": bool(os.environ.get("TRACE_TABLE")),
                "rate_limit_window_seconds": limiter.config.window_seconds,
            }
        )

    @app.middleware("http")
    async def log_request(request: Request, call_next):
        correlation_id = get_or_create_correlation_id(request)
        request.state.correlation_id = correlation_id
        start_time = time.time()
        try:
            response = await call_next(request)
        except Exception as exc:
            log_event(
                {
                    "event": "request_exception",
                    "method": request.method,
                    "path": request.url.path,
                    "latency_ms": record_latency(start_time),
                    "correlation_id": correlation_id,
                    "error": str(exc),
                }
            )
            raise
        attach_correlation_id(response, correlation_id)
        log_event(
            {
                "event": "request",
                "method": request.method,
                "path": request.url.path,
                "status_code": response.status_code,
                "latency_ms": record_latency(start_time),
                "correlation_id": correlation_id,
            }
        )
        return response

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok", "service": "feedback-api"}

    @app.get("/metrics")
    def metrics_endpoint() -> dict:
        return {"counters": metrics.counters}

    @app.post(
        "/feedback/submit",
        response_model=FeedbackResponse,
        responses={
            400: {"model": ErrorResponse},
            409: {"model": ErrorResponse},
            429: {"model": ErrorResponse},
            500: {"model": ErrorResponse},
        },
    )
    def submit_feedback(
        payload: FeedbackPayload, request: Request, response: Response
    ) -> FeedbackResponse:
        correlation_id = getattr(request.state, "correlation_id", None) or get_or_create_correlation_id(
            request
        )
        response.headers[CORRELATION_HEADER] = correlation_id
        config = limiter.config
        ip_key = f"ip:{request.client.host if request.client else 'unknown'}"
        allowed, retry_after = limiter.check(ip_key, config.limit_per_ip)
        if not allowed:
            response.headers["Retry-After"] = str(retry_after)
            metrics.inc("rate_limited_ip")
            log_event(
                {
                    "event": "rate_limited",
                    "scope": "ip",
                    "correlation_id": correlation_id,
                }
            )
            raise HTTPException(
                status_code=429,
                detail={
                    "error": "rate_limited",
                    "details": ["ip limit exceeded"],
                },
            )

        auth_header = request.headers.get("authorization") or ""
        if auth_header.lower().startswith("bearer "):
            token = auth_header[7:].strip()
            if token:
                token_key = f"token:{token}"
                allowed, retry_after = limiter.check(
                    token_key, config.limit_per_token
                )
                if not allowed:
                    response.headers["Retry-After"] = str(retry_after)
                    metrics.inc("rate_limited_token")
                    log_event(
                        {
                            "event": "rate_limited",
                            "scope": "token",
                            "correlation_id": correlation_id,
                        }
                    )
                    raise HTTPException(
                        status_code=429,
                        detail={
                            "error": "rate_limited",
                            "details": ["token limit exceeded"],
                        },
                    )
        errors = validate_payload(payload)
        if errors:
            metrics.inc("validation_failed")
            log_event(
                {
                    "event": "validation_failed",
                    "correlation_id": correlation_id,
                    "errors": errors,
                }
            )
            raise HTTPException(
                status_code=400,
                detail={"error": "validation_failed", "details": errors},
            )

        trace_policy = get_policy("TRACE_ID_POLICY", "warn")
        tracking_policy = get_policy("TRACKING_ID_POLICY", "warn")
        trace_regex = compile_regex("TRACE_ID_REGEX")
        tracking_regex = compile_regex("TRACKING_ID_REGEX")
        trace_max_age = get_int_env_optional("TRACE_ID_MAX_AGE_SECONDS")
        tracking_window = get_int_env_optional("TRACKING_ID_UNIQUENESS_SECONDS")

        trace_issues: list[str] = []
        trace_errors: list[str] = []
        if trace_policy != "off" and payload.trace_id:
            if trace_regex and not trace_regex.match(payload.trace_id):
                trace_issues.append("trace_id_format_invalid")
            if trace_max_age:
                trace_table = os.environ.get("TRACE_TABLE")
                warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID")
                if not trace_table or not warehouse_id:
                    trace_errors.append("trace_table_unconfigured")
                else:
                    try:
                        ts = get_trace_timestamp(trace_table, warehouse_id, payload.trace_id)
                        if not ts:
                            trace_issues.append("trace_id_not_found")
                        else:
                            age_seconds = (datetime.now(timezone.utc) - ts).total_seconds()
                            if age_seconds > trace_max_age:
                                trace_issues.append("trace_id_too_old")
                    except Exception as exc:
                        trace_errors.append(f"trace_lookup_failed:{exc}")

        tracking_issues: list[str] = []
        tracking_errors: list[str] = []
        if tracking_policy != "off":
            if tracking_regex and not tracking_regex.match(payload.tracking_id):
                tracking_issues.append("tracking_id_format_invalid")
            if tracking_window:
                try:
                    if tracking_id_recent_exists(
                        payload.tracking_id,
                        tracking_window,
                        pims=payload.pims,
                        user_id=payload.user_id,
                    ):
                        tracking_issues.append("tracking_id_recent_duplicate")
                except Exception as exc:
                    tracking_errors.append(f"tracking_id_lookup_failed:{exc}")

        if trace_errors or tracking_errors:
            metrics.inc("policy_check_failed")
            log_event(
                {
                    "event": "policy_check_failed",
                    "correlation_id": correlation_id,
                    "trace_errors": trace_errors,
                    "tracking_errors": tracking_errors,
                }
            )
            raise HTTPException(
                status_code=500,
                detail={
                    "error": "policy_check_failed",
                    "details": trace_errors + tracking_errors,
                },
            )

        if trace_issues and trace_policy == "strict":
            metrics.inc("trace_policy_rejected")
            raise HTTPException(
                status_code=400,
                detail={"error": "trace_id_policy_failed", "details": trace_issues},
            )
        if trace_issues and trace_policy == "warn":
            metrics.inc("trace_policy_warn")
            log_event(
                {
                    "event": "trace_policy_warn",
                    "correlation_id": correlation_id,
                    "issues": trace_issues,
                }
            )

        if tracking_issues and tracking_policy == "strict":
            metrics.inc("tracking_policy_rejected")
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "tracking_id_policy_failed",
                    "details": tracking_issues,
                },
            )
        if tracking_issues and tracking_policy == "warn":
            metrics.inc("tracking_policy_warn")
            log_event(
                {
                    "event": "tracking_policy_warn",
                    "correlation_id": correlation_id,
                    "issues": tracking_issues,
                }
            )

        if payload.user_id:
            user_key = f"user:{payload.user_id}"
            allowed, retry_after = limiter.check(
                user_key, config.limit_per_user
            )
            if not allowed:
                response.headers["Retry-After"] = str(retry_after)
                metrics.inc("rate_limited_user")
                log_event(
                    {
                        "event": "rate_limited",
                        "scope": "user",
                        "correlation_id": correlation_id,
                        "user_id": payload.user_id,
                    }
                )
                raise HTTPException(
                    status_code=429,
                    detail={
                        "error": "rate_limited",
                        "details": ["user limit exceeded"],
                    },
                )
        try:
            digest = payload_hash(payload)
            link_info = link_run(payload)
            metrics.inc(f"link_mode_{link_info.get('link_mode', 'unknown')}")
            feedback_id = write_feedback(payload, link_info, payload_hash_value=digest)
        except DuplicatePayloadError:
            metrics.inc("dedup_rejected")
            log_event(
                {
                    "event": "dedup_rejected",
                    "correlation_id": correlation_id,
                    "payload_hash": digest,
                }
            )
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "duplicate_payload",
                    "details": ["payload_hash already ingested"],
                },
            )
        except Exception as exc:  # pragma: no cover - runtime environment specific
            metrics.inc("ingestion_failed")
            log_event(
                {
                    "event": "ingestion_failed",
                    "correlation_id": correlation_id,
                    "error": str(exc),
                }
            )
            raise HTTPException(
                status_code=500,
                detail={"error": "ingestion_failed", "details": [str(exc)]},
            ) from exc
        metrics.inc("accepted")
        log_event(
            {
                "event": "accepted",
                "correlation_id": correlation_id,
                "tracking_id": payload.tracking_id,
                "trace_id": payload.trace_id,
                "link_mode": link_info.get("link_mode"),
            }
        )
        return FeedbackResponse(status="accepted", message="received", feedback_id=feedback_id)

    return app
