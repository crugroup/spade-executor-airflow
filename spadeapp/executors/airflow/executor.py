import logging
import requests
import base64
import os

from spadeapp.processes.executor import Executor, RunResult

logger = logging.getLogger(__name__)


class AirflowRunDAGExecutor(Executor):
    @classmethod
    def run(cls, system_params, user_params) -> RunResult:
        """Trigger a DAG to run."""

        airflow_url = os.environ.get("SPADE_AIRFLOW_URL")
        airflow_username = os.environ.get("SPADE_AIRFLOW_USERNAME")
        airflow_password = os.environ.get("SPADE_AIRFLOW_PASSWORD")
        if airflow_url is None or airflow_username is None or airflow_password is None:
            logger.error("Airflow URL, username, or password not set")
            return RunResult(result="failure")
        
        logger.info(f"Running Airflow DAG ID {system_params['dag_id']}")
        resp = requests.post(
            f"{airflow_url}/api/v1/dags/{system_params['dag_id']}/dag_runs",
            headers={
                "Authorization": f"Basic {
                    base64.b64encode(f'{airflow_username}:{airflow_password}').decode()
                }"
            },
            json={
                "conf": user_params["conf"],
            },
        )
        if resp.status_code != 200:
            logger.error(f"Failed to run DAG: {resp.text}")
            return RunResult(result="failure")

        return RunResult(result="success")