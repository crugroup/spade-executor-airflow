import logging
import requests
import base64
import os

from spadeapp.processes.history_provider import HistoryProvider
from spadeapp.processes.models import Process, ProcessRun

logger = logging.getLogger(__name__)

class ExampleHistoryProvider(HistoryProvider):
    @classmethod
    def get_runs(cls, process: Process, request, *args, **kwargs):
        """Trigger a DAG to run."""

        airflow_url = os.environ.get("SPADE_AIRFLOW_URL")
        airflow_username = os.environ.get("SPADE_AIRFLOW_USERNAME")
        airflow_password = os.environ.get("SPADE_AIRFLOW_PASSWORD")
        if airflow_url is None or airflow_username is None or airflow_password is None:
            logger.error("Airflow URL, username, or password not set")
            return ()
        
        logger.info(f"Retrieving Airflow runs for DAG ID {process.system_params['dag_id']}")
        resp = requests.get(
            f"{airflow_url}/api/v1/dags/{process.system_params['dag_id']}/dag_runs",
            headers={
                "Authorization": f"Basic {base64.b64encode(f'{airflow_username}:{airflow_password}')}"
            },
        )
        if resp.status_code != 200:
            logger.error(f"Failed to get DAG runs: {resp.text}")
            return ()
        data = resp.json()
        ret = []
        for run in data["dag_runs"]:
            process_run = ProcessRun(
                process=process,
                output=run,
            )
            if run["state"] == "success":
                process_run.status = ProcessRun.Statuses.FINISHED
                process_run.result = ProcessRun.Results.SUCCESS
            elif run["state"] == "failed":
                process_run.status = ProcessRun.Statuses.FINISHED
                process_run.result = ProcessRun.Results.FAILURE
            elif run["state"] == "running" or run["state"] == "restarting":
                process_run.status = ProcessRun.Statuses.RUNNING
            ret.append(process_run)
        return ret