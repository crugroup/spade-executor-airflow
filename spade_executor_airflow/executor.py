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
    
    @classmethod
    def run(cls, process, user_params) -> RunResult:
        """Trigger a DAG to run."""

        if cls.airflow_url is None or cls.airflow_username is None or cls.airflow_password is None:
            raise ValueError("Airflow URL, username, or password not set")

        logger.info(f"Running Airflow DAG ID {process.system_params['dag_id']}")
        auth_key = base64.b64encode(
            f"{cls.airflow_username}:{cls.airflow_password}".encode()
    ).decode()
        resp = requests.post(
            f"{cls.airflow_url}/api/v1/dags/{process.system_params['dag_id']}/dag_runs",
            headers={
                "Authorization": f"Basic {auth_key}",
            },
            json={
                "conf": user_params["conf"],
            },
        )
        if resp.status_code != 200:
            logger.error(f"Failed to run DAG: {resp.text}")
            return RunResult(process=process, status="error")

        return RunResult(process=process, status="running")
