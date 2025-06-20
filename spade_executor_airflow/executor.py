import logging
import os

import requests
from spadesdk.executor import Executor, RunResult

from . import utils

logger = logging.getLogger(__name__)


class AirflowRunDAGExecutor(Executor):
    airflow_url = os.environ.get("SPADE_AIRFLOW_URL")
    airflow_username = os.environ.get("SPADE_AIRFLOW_USERNAME")
    airflow_password = os.environ.get("SPADE_AIRFLOW_PASSWORD")
    airflow_verify_ssl = os.environ.get("SPADE_AIRFLOW_VERIFY_SSL", "true").lower() == "true"

    @classmethod
    def run(cls, process, user_params, user, *args, **kwargs) -> RunResult:
        """Trigger a DAG to run."""

        if cls.airflow_url is None or cls.airflow_username is None or cls.airflow_password is None:
            raise ValueError("Airflow URL, username, or password not set")

        dag_id = user_params.pop("dag_id", None) or process.system_params.get("dag_id")
        if not dag_id:
            return RunResult(process=process, status=RunResult.Status.FAILED, error_message="No DAG ID provided")

        logger.info(f"Running Airflow DAG ID {dag_id}")
        token = utils.request_airflow_token(
            cls.airflow_url,
            cls.airflow_username,
            cls.airflow_password,
            verify_ssl=cls.airflow_verify_ssl,
        )
        params = user_params or {}
        params["spade__user_id"] = user.id
        params["spade__user_email"] = user.email
        logger.info(f"Sending request to {cls.airflow_url}/api/v1/dags/{dag_id}/dagRuns")
        logger.info(f"Params: {params}")
        resp = requests.post(
            f"{cls.airflow_url}/api/v2/dags/{dag_id}/dagRuns",
            headers={
                "Authorization": f"Bearer {token}",
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
