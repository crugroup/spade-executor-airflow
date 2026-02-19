import logging
from datetime import datetime, timezone

import requests
from spadesdk.executor import Executor, RunResult

from . import utils

logger = logging.getLogger(__name__)


class AirflowRunDAGExecutor(Executor):
    @classmethod
    def run(cls, process, user_params: dict, user, *args, **kwargs) -> RunResult:
        """Trigger a DAG to run."""

        system_params = process.system_params

        if "airflow_base_url" not in system_params:
            raise ValueError("Airflow base URL missing from system params")
        if "airflow_username" not in system_params:
            raise ValueError("Airflow username missing from system params")
        if "airflow_password" not in system_params:
            raise ValueError("Airflow password missing from system params")

        airflow_base_url = system_params["airflow_base_url"]
        airflow_username = system_params["airflow_username"]
        airflow_password = system_params["airflow_password"]
        airflow_verify_ssl = system_params.get("airflow_verify_ssl", "true") == "true"
        airflow_allow_multiple_runs = system_params.get("airflow_allow_multiple_runs", "false") == "true"

        if not user_params.pop("confirmation", True):
            return RunResult(process=process, status=RunResult.Status.FAILED, error_message="User confirmation missing")

        dag_id = user_params.pop("dag_id", None) or process.system_params.get("dag_id")
        if not dag_id:
            return RunResult(process=process, status=RunResult.Status.FAILED, error_message="No DAG ID provided")

        token = utils.request_airflow_token(
            airflow_base_url,
            airflow_username,
            airflow_password,
            verify_ssl=airflow_verify_ssl,
        )

        if not airflow_allow_multiple_runs:
            logger.info(f"Checking for existing in-progress run for DAG {dag_id}")
            runs = utils.get_dag_runs(airflow_base_url, token, dag_id, limit=1, verify_ssl=airflow_verify_ssl)
            if len(runs) > 0 and runs[0]["state"] in ("running", "restarting"):
                return RunResult(
                    process=process, status=RunResult.Status.FAILED, error_message="DAG is already running"
                )

        logger.info(f"Running Airflow DAG {dag_id}")
        params = user_params or {}
        params["spade__user_id"] = user.id
        params["spade__user_email"] = user.email
        logger.info(f"Sending request to {airflow_base_url}/api/v1/dags/{dag_id}/dagRuns")
        logger.info(f"Params: {params}")
        resp = requests.post(
            f"{airflow_base_url}/api/v2/dags/{dag_id}/dagRuns",
            headers={
                "Authorization": f"Bearer {token}",
            },
            json={
                "conf": params,
                "logical_date": datetime.now(timezone.utc).isoformat(),
            },
            verify=airflow_verify_ssl,
        )
        if resp.status_code != 200:
            logger.error(f"Failed to run DAG: {resp.text}")
            return RunResult(process=process, status=RunResult.Status.FAILED, error_message=resp.text)

        return RunResult(process=process, status=RunResult.Status.RUNNING)
