import logging

import requests

logger = logging.getLogger(__name__)


def request_airflow_token(
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
    token = resp.cookies.get("_token")
    if not token:
        raise ValueError("Failed to get Airflow token: No token in response")
    return token


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
