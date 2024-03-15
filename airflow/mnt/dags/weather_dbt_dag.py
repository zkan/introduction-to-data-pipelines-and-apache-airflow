from airflow.utils import timezone

from cosmos import DbtDag, ProjectConfig, ProfileConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping


profile_config = ProfileConfig(
    profile_name="weather",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="my_postgres_conn",
        profile_args={"schema": "dbt_kan"},
    ),
)

weather_dbt_dag = DbtDag(
    dag_id="weather_dbt_dag",
    schedule_interval="@daily",
    start_date=timezone.datetime(2022, 11, 27),
    catchup=False,
    project_config=ProjectConfig("/opt/airflow/dbt/weather"),
    profile_config=profile_config
)
