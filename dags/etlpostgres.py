from airflow import DAG
from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from datetime import datetime, timedelta
from pathlib import Path


profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="airflow_db",
        profile_args={"schema": "dev"},
    )
)


default_args = {
    'owner': 'airflow',
    'retries': 1,
}


with DAG(
    dag_id='dbt_jaffle_shop_pipeline',
    default_args=default_args,
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    dbt_group = DbtTaskGroup(
        group_id="jaffle_shop",
        project_config=ProjectConfig(
            str(Path(__file__).parent / "dbtproject" / "jaffle-shop")
        ),
        execution_config=ExecutionConfig(
            dbt_executable_path = "/usr/local/airflow/dbt_venv/bin/dbt"
        ),
        profile_config=profile_config,
    )
