from airflow import DAG
from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig, ExecutionConfig
from cosmos.profiles import TrinoCertificateProfileMapping, TrinoLDAPProfileMapping, TrinoJWTProfileMapping
from datetime import datetime, timedelta
from pathlib import Path
import requests
from airflow.providers.trino.hooks.trino import TrinoHook
from airflow.decorators import task
from airflow.models import Variable
from zoneinfo import ZoneInfo


profile_config = ProfileConfig(
    profile_name="ecommerce_dbt",
    target_name="trino",
    # profile_mapping=TrinoLDAPProfileMapping(
    #     conn_id="trino_conn",
    #     profile_args={"database": "iceberg","schema": "bronze"}
    # ),
    profiles_yml_filepath=str(Path(__file__).parent / "dbtproject" / "ecommerce_dbt" / "profiles.yml")
)


default_args = {
    'owner': 'airflow',
    'retries': 1,
}


with DAG(
    dag_id='trino_analysis_pipeline',
    default_args=default_args,
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    

    etl_trino = DbtTaskGroup(
        group_id="etl_trino",
        project_config=ProjectConfig(
            str(Path(__file__).parent / "dbtproject" / "ecommerce_dbt")
        ),
        execution_config=ExecutionConfig(
            dbt_executable_path = "/usr/local/airflow/dbt_venv/bin/dbt"
        ),
        profile_config=profile_config,
    )