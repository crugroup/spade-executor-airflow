import requests


def request_airflow_token(
    airflow_url: str,
    airflow_username: str,
    airflow_password: str,
    verify_ssl: bool = True,
) -> str:
    """Request an Airflow token using the API login endpoint."""
    resp = requests.post(
        f"{airflow_url}/auth/token",
        json={
            "username": airflow_username,
            "password": airflow_password,
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
