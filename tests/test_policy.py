import pytest

from app.feedback_api.policy import compile_regex, get_int_env_optional, get_policy


def test_get_policy_valid(monkeypatch):
    monkeypatch.setenv("TEST_POLICY", "strict")
    assert get_policy("TEST_POLICY") == "strict"


def test_get_policy_invalid(monkeypatch):
    monkeypatch.setenv("TEST_POLICY", "invalid")
    with pytest.raises(RuntimeError):
        get_policy("TEST_POLICY")


def test_compile_regex_invalid(monkeypatch):
    monkeypatch.setenv("TEST_REGEX", "[")
    with pytest.raises(RuntimeError):
        compile_regex("TEST_REGEX")


def test_get_int_env_optional(monkeypatch):
    monkeypatch.setenv("TEST_INT", "5")
    assert get_int_env_optional("TEST_INT") == 5
