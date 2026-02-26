"""Airflow DAG: Azure Blob Storage → GCS bronze layer copy using rclone.

This DAG submits a Dataproc Serverless batch job that runs rclone_copy.py
to transfer data from an Azure storage container to a GCS bucket.

Root cause of the original failure
-----------------------------------
rclone requires remotes ('azure', 'google') to be configured either via a
rclone.conf file or via RCLONE_CONFIG_<REMOTE>_<KEY> environment variables.
Dataproc workers have neither by default, so the rclone command fails with
exit code 1 without a descriptive error.

Fix
---
This DAG reads Azure credentials from an Airflow connection and passes them
as environment variables inside the Dataproc batch execution_config.
GCS authentication uses the worker's Application Default Credentials
(Dataproc service account) — no explicit Google rclone config is needed.

Prerequisites
-------------
1. Upload scripts-gcp/rclone_copy.py to GCS:
       gsutil cp scripts-gcp/rclone_copy.py gs://<SCRIPTS_BUCKET>/scripts-gcp/

2. Create an Airflow connection 'azure_blob_storage':
       conn_id   : azure_blob_storage
       conn_type : Generic
       login     : <Azure storage account name>
       password  : <Azure storage account key>

3. Set Airflow Variables (Admin → Variables):
       gcp_project_id          : your-gcp-project
       gcp_region              : us-central1
       dataproc_service_account: sa@your-project.iam.gserviceaccount.com
       scripts_bucket          : your-scripts-bucket
       gcp_subnetwork          : (optional) projects/.../subnetworks/...

Trigger params (all optional, fall back to defaults below):
    source_path       Rclone source  (e.g. azure:bronze/POPUS/)
    destination_path  Rclone dest    (e.g. google:bucket/v3/data)
    transfers         Parallel file transfers   (default 16)
    checkers          Parallel checkers         (default 32)
    dry_run           Boolean — list files only, no copy
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.models.param import Param

# ── Defaults ──────────────────────────────────────────────────────────────────

DEFAULT_SOURCE = "azure:bronze/POPUS/"
DEFAULT_DESTINATION = "google:dnane1gcs-pfa-POPUS-bronze/v3/data"
AZURE_CONN_ID = "azure_blob_storage"
DATAPROC_RUNTIME_VERSION = "2.2"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _azure_credentials(conn_id: str) -> tuple[str, str]:
    """Fetch Azure account name and key from an Airflow connection.

    Args:
        conn_id: Airflow connection identifier.

    Returns:
        (account_name, account_key)

    Raises:
        ValueError: If login or password is not set on the connection.
    """
    conn = BaseHook.get_connection(conn_id)
    if not conn.login or not conn.password:
        raise ValueError(
            f"Airflow connection '{conn_id}' must have "
            "'login' (storage account name) and 'password' (storage account key)."
        )
    return conn.login, conn.password


def _rclone_env_vars(azure_account: str, azure_key: str) -> dict[str, str]:
    """Build the environment variables that configure rclone remotes.

    rclone reads RCLONE_CONFIG_<REMOTE>_<KEY> at startup, so no
    rclone.conf file is required on the Dataproc worker.

    GCS auth is handled by the Dataproc service account (ADC),
    so only the remote type needs to be declared for the 'google' remote.

    Args:
        azure_account: Azure Blob Storage account name.
        azure_key: Azure Blob Storage account key.

    Returns:
        Dict mapping env var names to values.
    """
    return {
        # Azure Blob Storage remote
        "RCLONE_CONFIG_AZURE_TYPE": "azureblob",
        "RCLONE_CONFIG_AZURE_ACCOUNT": azure_account,
        "RCLONE_CONFIG_AZURE_KEY": azure_key,
        # GCS remote — authentication via ADC on the Dataproc worker
        "RCLONE_CONFIG_GOOGLE_TYPE": "google cloud storage",
        "RCLONE_CONFIG_GOOGLE_OBJECT_ACL": "bucketOwnerFullControl",
        "RCLONE_CONFIG_GOOGLE_BUCKET_ACL": "bucketOwnerFullControl",
    }


def _batch_id(prefix: str = "azure-gcp-copy") -> str:
    """Generate a unique Dataproc batch ID (must match [a-z][a-z0-9-]*)."""
    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    raw = f"{prefix}-{ts}"
    safe = re.sub(r"[^a-z0-9-]", "-", raw.lower())[:63].strip("-")
    return safe


# ── DAG ───────────────────────────────────────────────────────────────────────

@dag(
    dag_id="azure_to_gcp_copy",
    description="Copy files from Azure Blob Storage to GCS using rclone on Dataproc Serverless",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["ingestion", "azure", "gcs", "rclone", "dataproc"],
    params={
        "source_path": Param(
            default=DEFAULT_SOURCE,
            type="string",
            description="rclone source path, e.g. azure:container/path/",
        ),
        "destination_path": Param(
            default=DEFAULT_DESTINATION,
            type="string",
            description="rclone destination path, e.g. google:bucket/path",
        ),
        "transfers": Param(default=16, type="integer", description="Parallel transfers"),
        "checkers": Param(default=32, type="integer", description="Parallel checkers"),
        "dry_run": Param(default=False, type="boolean", description="Dry run only"),
    },
)
def azure_to_gcp_copy():
    """Copy data from Azure Blob Storage to GCS bronze layer using rclone."""

    @task
    def build_batch_config(**context) -> dict:
        """Resolve params, load credentials, and assemble the Dataproc batch spec.

        Returns:
            Dict with 'batch_id' and 'batch' (Dataproc batch resource dict).
        """
        params = context["params"]

        source = params["source_path"]
        destination = params["destination_path"]
        transfers = int(params["transfers"])
        checkers = int(params["checkers"])
        dry_run = bool(params["dry_run"])

        # Credentials from Airflow connection
        azure_account, azure_key = _azure_credentials(AZURE_CONN_ID)
        env_vars = _rclone_env_vars(azure_account, azure_key)

        # Infrastructure config from Airflow Variables
        project_id = Variable.get("gcp_project_id")
        service_account = Variable.get("dataproc_service_account")
        scripts_bucket = Variable.get("scripts_bucket")
        subnetwork = Variable.get("gcp_subnetwork", default_var="")

        script_uri = f"gs://{scripts_bucket}/scripts-gcp/rclone_copy.py"

        # rclone CLI args passed to the Python script
        script_args = [
            "--source", source,
            "--destination", destination,
            "--transfers", str(transfers),
            "--checkers", str(checkers),
        ]
        if dry_run:
            script_args.append("--dry-run")

        execution_config: dict = {
            "service_account": service_account,
            "env_vars": env_vars,  # ← rclone remote config injected here
        }
        if subnetwork:
            execution_config["subnetwork_uri"] = subnetwork

        batch = {
            "pyspark_batch": {
                "main_python_file_uri": script_uri,
                "args": script_args,
            },
            "runtime_config": {
                "version": DATAPROC_RUNTIME_VERSION,
            },
            "environment_config": {
                "execution_config": execution_config,
            },
        }

        return {
            "batch_id": _batch_id(),
            "batch": batch,
            "project_id": project_id,
            "region": Variable.get("gcp_region", default_var="us-central1"),
        }

    @task
    def submit_dataproc_batch(config: dict) -> str:
        """Submit the Dataproc Serverless batch job and wait for completion.

        Args:
            config: Output of build_batch_config.

        Returns:
            Completed batch job ID.

        Raises:
            AirflowException: If the batch job fails.
        """
        from airflow.providers.google.cloud.hooks.dataproc import DataprocHook

        hook = DataprocHook(gcp_conn_id="google_cloud_default")

        batch_id = config["batch_id"]
        project_id = config["project_id"]
        region = config["region"]

        hook.create_batch(
            project_id=project_id,
            region=region,
            batch=config["batch"],
            batch_id=batch_id,
        )

        # Poll until terminal state
        import time
        from google.cloud.dataproc_v1 import Batch

        terminal = {
            Batch.State.SUCCEEDED,
            Batch.State.FAILED,
            Batch.State.CANCELLED,
        }

        while True:
            batch_info = hook.get_batch(
                project_id=project_id,
                region=region,
                batch_id=batch_id,
            )
            state = batch_info.state

            if state in terminal:
                break

            time.sleep(30)

        if state != Batch.State.SUCCEEDED:
            raise Exception(
                f"Dataproc batch '{batch_id}' ended with state {state.name}. "
                f"Message: {batch_info.state_message}"
            )

        return batch_id

    cfg = build_batch_config()
    submit_dataproc_batch(cfg)


azure_to_gcp_copy()
