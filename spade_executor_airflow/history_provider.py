import logging
import os
from datetime import datetime, timezone

import requests
from spadesdk.executor import Process, RunResult
from spadesdk.history_provider import HistoryProvider

from . import utils

logger = logging.getLogger(__name__)


class AirflowRunHistoryProvider(HistoryProvider):
    airflow_url = os.environ.get("SPADE_AIRFLOW_URL")
    airflow_username = os.environ.get("SPADE_AIRFLOW_USERNAME")
    airflow_password = os.environ.get("SPADE_AIRFLOW_PASSWORD")
    airflow_verify_ssl = os.environ.get("SPADE_AIRFLOW_VERIFY_SSL", "true").lower() == "true"

    @classmethod
    def get_runs(cls, process: Process, request, *args, **kwargs):
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

        token = utils.request_airflow_token(
            airflow_base_url,
            airflow_username,
            airflow_password,
            verify_ssl=airflow_verify_ssl,
        )

        dag_ids = []
        if "dag_ids" in process.system_params:
            dag_ids = process.system_params["dag_ids"]
        elif "dag_id" in process.system_params:
            dag_ids = [process.system_params["dag_id"]]

        ret = []
        for dag_id in dag_ids:
            logger.info(f"Retrieving Airflow runs for DAG ID {dag_id}")

            resp = requests.get(
                f"{cls.airflow_url}/api/v2/dags/{dag_id}/dagRuns?order_by=-logical_date",
                headers={"Authorization": f"Bearer {token}"},
                verify=cls.airflow_verify_ssl,
            )
            if resp.status_code != 200:
                logger.error(f"Failed to get DAG runs: {resp.text}")
                return ()
            data = resp.json()

            for run in data["dag_runs"]:
                status = RunResult.Status.NEW
                result = None
                if run["state"] == "success":
                    status = RunResult.Status.FINISHED
                    result = RunResult.Result.SUCCESS
                elif run["state"] == "failed":
                    status = RunResult.Status.FINISHED
                    result = RunResult.Result.FAILED
                elif run["state"] == "running" or run["state"] == "restarting":
                    status = RunResult.Status.RUNNING
                    result = None
                process_run = RunResult(
                    process=process,
                    output=run,
                    status=status,
                    result=result,
                    created_at=(
                        datetime.strptime(run.get("start_date"), "%Y-%m-%dT%H:%M:%S.%f%z")
                        if run.get("start_date")
                        else None
                    ),
                    user_id=run["conf"].get("spade__user_id"),
                )
                ret.append(process_run)
        ret.sort(
            key=lambda r: r.created_at if r.created_at is not None else datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )
        return ret
