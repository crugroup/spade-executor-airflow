import base64
import logging
import os

import requests
from spadesdk.executor import Executor, RunResult

logger = logging.getLogger(__name__)


class AirflowRunDAGExecutor(Executor):
    airflow_url = os.environ.get("SPADE_AIRFLOW_URL")
    airflow_username = os.environ.get("SPADE_AIRFLOW_USERNAME")
    airflow_password = os.environ.get("SPADE_AIRFLOW_PASSWORD")
    airflow_verify_ssl = os.environ.get("SPADE_AIRFLOW_VERIFY_SSL", "true").lower() == "true"

    @classmethod
    def run(cls, process, user_params, user_id) -> RunResult:
        """Trigger a DAG to run."""

        if cls.airflow_url is None or cls.airflow_username is None or cls.airflow_password is None:
            raise ValueError("Airflow URL, username, or password not set")

        logger.info(f"Running Airflow DAG ID {process.system_params['dag_id']}")
        auth_key = base64.b64encode(f"{cls.airflow_username}:{cls.airflow_password}".encode()).decode()
        params = user_params or {}
        params["spade__user_id"] = user_id
        logger.info(f"Sending request to {cls.airflow_url}/api/v1/dags/{process.system_params['dag_id']}/dagRuns")
        logger.info(f"Params: {params}")
        resp = requests.post(
            f"{cls.airflow_url}/api/v1/dags/{process.system_params['dag_id']}/dagRuns",
            headers={
                "Authorization": f"Basic {auth_key}",
            },
            json={
                "conf": params,
            },
            verify=cls.airflow_verify_ssl,
        )
        if resp.status_code != 200:
            logger.error(f"Failed to run DAG: {resp.text}")
            return RunResult(process=process, status=RunResult.Status.FAILED, error_message=resp.text)

        return RunResult(process=process, status=RunResult.Status.RUNNING)
