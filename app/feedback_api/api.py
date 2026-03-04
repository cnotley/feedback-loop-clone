"""FastAPI application and request handlers for feedback ingestion"""

# pylint: disable=too-many-locals,too-many-branches,too-many-statements
# pylint: disable=too-many-nested-blocks,broad-exception-caught,raise-missing-from,line-too-long

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
from .identifiers import validate_uc_table_name


def _log_rate_limited(scope: str, correlation_id: str, **extra) -> None:
    """Log a rate limited event with standard fields"""
    payload = {
        "event": "rate_limited",
        "scope": scope,
        "correlation_id": correlation_id,
    }
    payload.update(extra)
    log_event(payload)


def _enforce_rate_limit(
    *,
    limiter: RateLimiter,
    key: str,
    limit: int,
    response: Response,
    correlation_id: str,
    scope: str,
    metric_name: str,
    detail_message: str,
    **log_fields,
) -> None:
    """Apply rate limiting and raise HTTPException on violation"""
    allowed, retry_after = limiter.check(key, limit)
    if allowed:
        return
    response.headers["Retry-After"] = str(retry_after)
    metrics.inc(metric_name)
    _log_rate_limited(scope, correlation_id, **log_fields)
    raise HTTPException(
        status_code=429,
        detail={
            "error": "rate_limited",
            "details": [detail_message],
        },
    )


def _validate_payload_or_raise(payload: FeedbackPayload, correlation_id: str) -> None:
    """Validate the payload and raise HTTPException on errors"""
    errors = validate_payload(payload)
    if not errors:
        return
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


def _evaluate_trace_policy(payload: FeedbackPayload) -> tuple[str, list[str], list[str]]:
    """Evaluate trace policy and return policy, issues, and errors"""
    trace_policy = get_policy("TRACE_ID_POLICY", "warn")
    trace_regex = compile_regex("TRACE_ID_REGEX")
    trace_max_age = get_int_env_optional("TRACE_ID_MAX_AGE_SECONDS")

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
    return trace_policy, trace_issues, trace_errors


def _evaluate_tracking_policy(
    payload: FeedbackPayload,
) -> tuple[str, list[str], list[str]]:
    """Evaluate tracking policy and return policy, issues, and errors"""
    tracking_policy = get_policy("TRACKING_ID_POLICY", "warn")
    tracking_regex = compile_regex("TRACKING_ID_REGEX")
    tracking_window = get_int_env_optional("TRACKING_ID_UNIQUENESS_SECONDS")

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
    return tracking_policy, tracking_issues, tracking_errors


def _handle_policy_results(
    *,
    correlation_id: str,
    trace_policy: str,
    trace_issues: list[str],
    trace_errors: list[str],
    tracking_policy: str,
    tracking_issues: list[str],
    tracking_errors: list[str],
) -> None:
    """Apply policy decisions and raise HTTPException when required"""
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


def _ingest_feedback(
    payload: FeedbackPayload, correlation_id: str
) -> tuple[str, dict, str]:
    """Persist feedback and return (feedback_id, link_info, payload_hash)"""
    try:
        digest = payload_hash(payload)
        link_info = link_run(payload)
        metrics.inc(f"link_mode_{link_info.get('link_mode', 'unknown')}")
        feedback_id = write_feedback(payload, link_info, payload_hash_value=digest)
        return feedback_id, link_info, digest
    except DuplicatePayloadError as exc:
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
        ) from exc
    except Exception as exc:
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


def create_app() -> FastAPI:
    """Build and configure the Feedback API application"""
    app = FastAPI(title="Feedback API", version="v1")
    limiter = RateLimiter()

    @app.on_event("startup")
    def validate_startup() -> None:
        """Validate required environment and log startup configuration"""
        required = ["FEEDBACK_TABLE", "DATABRICKS_WAREHOUSE_ID"]
        missing = [name for name in required if not os.environ.get(name)]
        if missing:
            raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")
        validate_uc_table_name("FEEDBACK_TABLE", os.environ["FEEDBACK_TABLE"])
        trace_table = os.environ.get("TRACE_TABLE")
        if trace_table:
            validate_uc_table_name("TRACE_TABLE", trace_table)
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
        """Attach correlation ID, log request, and record latency"""
        correlation_id = get_or_create_correlation_id(request)
        request.state.correlation_id = correlation_id
        start_time = time.time()
        try:
            response = await call_next(request)
        except Exception as exc:
            latency_ms = record_latency(start_time)
            metrics.inc("requests_total")
            metrics.inc("requests_exception")
            metrics.inc("requests_errors_5xx")
            metrics.observe_ms("request_latency_ms", latency_ms)
            log_event(
                {
                    "event": "request_exception",
                    "method": request.method,
                    "path": request.url.path,
                    "latency_ms": latency_ms,
                    "correlation_id": correlation_id,
                    "error": str(exc),
                }
            )
            raise
        attach_correlation_id(response, correlation_id)
        latency_ms = record_latency(start_time)
        metrics.inc("requests_total")
        status_code = response.status_code
        status_class = status_code // 100
        metrics.inc(f"requests_status_{status_class}xx")
        if 400 <= status_code < 500:
            metrics.inc("requests_errors_4xx")
        elif status_code >= 500:
            metrics.inc("requests_errors_5xx")
        metrics.observe_ms("request_latency_ms", latency_ms)
        log_event(
            {
                "event": "request",
                "method": request.method,
                "path": request.url.path,
                "status_code": status_code,
                "latency_ms": latency_ms,
                "correlation_id": correlation_id,
            }
        )
        return response

    @app.get("/health")
    def health() -> dict:
        """Basic health check endpoint"""
        return {"status": "ok", "service": "feedback-api"}

    @app.get("/metrics")
    def metrics_endpoint() -> dict:
        """Expose JSON friendly metrics for diagnostics and monitoring"""
        return metrics.snapshot()

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
        """Validate, link, and persist a feedback payload"""
        correlation_id = getattr(request.state, "correlation_id", None) or get_or_create_correlation_id(
            request
        )
        response.headers[CORRELATION_HEADER] = correlation_id
        config = limiter.config
        ip_key = f"ip:{request.client.host if request.client else 'unknown'}"
        _enforce_rate_limit(
            limiter=limiter,
            key=ip_key,
            limit=config.limit_per_ip,
            response=response,
            correlation_id=correlation_id,
            scope="ip",
            metric_name="rate_limited_ip",
            detail_message="ip limit exceeded",
        )

        auth_header = request.headers.get("authorization") or ""
        if auth_header and not auth_header.lower().startswith("bearer "):
            metrics.inc("invalid_auth_attempts")
            log_event(
                {
                    "event": "auth_failure",
                    "reason": "invalid_auth_scheme",
                    "correlation_id": correlation_id,
                }
            )
        if auth_header.lower().startswith("bearer "):
            token = auth_header[7:].strip()
            if token:
                token_key = f"token:{token}"
                _enforce_rate_limit(
                    limiter=limiter,
                    key=token_key,
                    limit=config.limit_per_token,
                    response=response,
                    correlation_id=correlation_id,
                    scope="token",
                    metric_name="rate_limited_token",
                    detail_message="token limit exceeded",
                )
            else:
                metrics.inc("invalid_auth_attempts")
                log_event(
                    {
                        "event": "auth_failure",
                        "reason": "empty_bearer_token",
                        "correlation_id": correlation_id,
                    }
                )

        _validate_payload_or_raise(payload, correlation_id)

        trace_policy, trace_issues, trace_errors = _evaluate_trace_policy(payload)
        tracking_policy, tracking_issues, tracking_errors = _evaluate_tracking_policy(
            payload
        )
        _handle_policy_results(
            correlation_id=correlation_id,
            trace_policy=trace_policy,
            trace_issues=trace_issues,
            trace_errors=trace_errors,
            tracking_policy=tracking_policy,
            tracking_issues=tracking_issues,
            tracking_errors=tracking_errors,
        )

        if payload.user_id:
            user_key = f"user:{payload.user_id}"
            _enforce_rate_limit(
                limiter=limiter,
                key=user_key,
                limit=config.limit_per_user,
                response=response,
                correlation_id=correlation_id,
                scope="user",
                metric_name="rate_limited_user",
                detail_message="user limit exceeded",
                user_id=payload.user_id,
            )

        feedback_id, link_info, _digest = _ingest_feedback(payload, correlation_id)
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