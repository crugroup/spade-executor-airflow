import base64
import hashlib
import json
import logging
import threading
import time

import requests

logger = logging.getLogger(__name__)

# A dashboard load asks for the history of every provider-backed process, and each
# process used to log in to Airflow separately. Reuse a token per credentials until
# shortly before it expires, which removes that login burst.
_DEFAULT_TOKEN_TTL = 300
_MAX_TOKEN_TTL = 900
_token_cache: dict[tuple[str, str, str, bool], tuple[str, float]] = {}
_token_cache_lock = threading.Lock()


def _token_cache_key(base_url: str, username: str, password: str, verify_ssl: bool) -> tuple[str, str, str, bool]:
    return (base_url, username, hashlib.sha256(password.encode()).hexdigest(), verify_ssl)


def _token_ttl(token: str) -> float:
    """Return how long a token can be reused, preferring its JWT expiry.

    The cap keeps a revoked token from being reused for the whole of a long expiry.
    """
    try:
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        claims = json.loads(base64.urlsafe_b64decode(payload.encode()))
        expires_in = float(claims["exp"]) - time.time() - 60
    except (IndexError, KeyError, TypeError, ValueError):
        return _DEFAULT_TOKEN_TTL
    return max(0.0, min(expires_in, _MAX_TOKEN_TTL))


def request_airflow_token(
    base_url: str,
    username: str,
    password: str,
    verify_ssl: bool = True,
) -> str:
    """Return an Airflow token, reusing a cached one while it is still valid."""
    key = _token_cache_key(base_url, username, password, verify_ssl)
    now = time.monotonic()
    with _token_cache_lock:
        cached = _token_cache.get(key)
        if cached is not None and cached[1] > now:
            return cached[0]

        # Hold the lock while logging in so concurrent callers share a single request.
        token = _fetch_airflow_token(base_url, username, password, verify_ssl)
        _token_cache[key] = (token, now + _token_ttl(token))
        return token


def _fetch_airflow_token(
    base_url: str,
    username: str,
    password: str,
    verify_ssl: bool = True,
) -> str:
    """Request an Airflow token using the API login endpoint."""
    resp = requests.post(
        f"{base_url}/auth/token",
        json={
            "username": username,
            "password": password,
        },
        verify=verify_ssl,
        allow_redirects=False,
    )
    if resp.status_code > 400:
        raise ValueError(f"Failed to get Airflow token: {resp.text}")

    # Primary: extract token from JSON response {"access_token": "..."}
    try:
        token = resp.json().get("access_token")
        if token:
            return token
    except ValueError:
        pass

    # Legacy fallback: extract token from cookies (to be removed)
    token = resp.cookies.get("_token")
    if token:
        return token

    raise ValueError("Failed to get Airflow token: No token in response")


def get_dag_runs(
    base_url: str,
    token: str,
    dag_id: str,
    *,
    verify_ssl: bool = True,
    limit: int | None = None,
    offset: int | None = None,
) -> list[dict[str, str]]:
    request_url = f"{base_url}/api/v2/dags/{dag_id}/dagRuns?order_by=-logical_date"
    if limit is not None:
        request_url += f"&limit={limit}"
    if offset is not None:
        request_url += f"&offset={offset}"
    resp = requests.get(
        request_url,
        headers={"Authorization": f"Bearer {token}"},
        verify=verify_ssl,
    )
    if resp.status_code != 200:
        logger.error(f"Failed to get DAG runs: {resp.text}")
        return []
    return resp.json()["dag_runs"]


def get_run_errors(
    base_url: str,
    token: str,
    dag_id: str,
    dag_run_id: str,
    verify_ssl: bool = True,
) -> str:
    request_url = f"{base_url}/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances?state=failed"
    resp = requests.get(
        request_url,
        headers={"Authorization": f"Bearer {token}"},
        verify=verify_ssl,
    )
    if resp.status_code != 200:
        logger.error(f"Failed to get DAG run errors: {resp.text}")
        return ""
    task_instances = resp.json()["task_instances"]
    failed_tasks = [task["task_id"] for task in task_instances]
    return "Failed tasks: " + ", ".join(failed_tasks) if failed_tasks else "No failed tasks"
