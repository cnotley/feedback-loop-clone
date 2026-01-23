from app.feedback_api.rate_limit import RateLimitConfig, RateLimiter


def test_rate_limit_allows_and_blocks():
    limiter = RateLimiter(
        RateLimitConfig(
            window_seconds=60,
            limit_per_user=1,
            limit_per_token=1,
            limit_per_ip=1,
        )
    )
    allowed, retry_after = limiter.check("key", 1)
    assert allowed is True
    assert retry_after == 0
    allowed, retry_after = limiter.check("key", 1)
    assert allowed is False
    assert retry_after >= 1
