import base64
import logging
import os

import requests
from spadesdk.executor import Process, RunResult
from spadesdk.history_provider import HistoryProvider

logger = logging.getLogger(__name__)


class AirflowRunHistoryProvider(HistoryProvider):
    airflow_url = os.environ.get("SPADE_AIRFLOW_URL")
    airflow_username = os.environ.get("SPADE_AIRFLOW_USERNAME")
    airflow_password = os.environ.get("SPADE_AIRFLOW_PASSWORD")
    if airflow_url is None or airflow_username is None or airflow_password is None:
        logger.error("Airflow URL, username, or password not set")

    @classmethod
    def get_runs(cls, process: Process, request, *args, **kwargs):
        """Trigger a DAG to run."""

        logger.info(f"Retrieving Airflow runs for DAG ID {process.system_params['dag_id']}")
        auth_key = base64.b64encode(f"{cls.airflow_username}:{cls.airflow_password}").decode()
        resp = requests.get(
            f"{cls.airflow_url}/api/v1/dags/{process.system_params['dag_id']}/dag_runs",
            headers={"Authorization": f"Basic {auth_key}"},
        )
        if resp.status_code != 200:
            logger.error(f"Failed to get DAG runs: {resp.text}")
            return ()
        data = resp.json()
        ret = []
        for run in data["dag_runs"]:
            process_run = RunResult(
                process=process,
                output=run,
            )
            if run["state"] == "success":
                process_run.status = "finished"
                process_run.result = "success"
            elif run["state"] == "failed":
                process_run.status = "finished"
                process_run.result = "failed"
            elif run["state"] == "running" or run["state"] == "restarting":
                process_run.status = "running"
            ret.append(process_run)
        return ret
