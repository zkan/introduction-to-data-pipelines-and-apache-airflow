import json

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone

import great_expectations as gx
import pandas as pd
import requests
from great_expectations.dataset import PandasDataset


def _get_weather_data(**context):
    # API_KEY = os.environ.get("WEATHER_API_KEY")
    API_KEY = Variable.get("weather_api_key")

    name = Variable.get("name")
    print(f"Hello, {name}")

    print(context)
    print(context["execution_date"])
    ds = context["ds"]
    print(ds)

    payload = {
        "q": "bangkok",
        "appid": API_KEY,
        "units": "metric"
    }
    url = f"https://api.openweathermap.org/data/2.5/weather"
    response = requests.get(url, params=payload)
    print(response.url)

    data = response.json()
    print(data)

    timestamp = context["execution_date"]
    with open(f"/opt/airflow/dags/weather_data_{timestamp}.json", "w") as f:
        json.dump(data, f)

    return f"/opt/airflow/dags/weather_data_{timestamp}.json"


def _validate_temperature(**context):
    ti = context["ti"]
    file_name = ti.xcom_pull(task_ids="get_weather_data", key="return_value")

    with open(file_name, "r") as f:
        data = json.load(f)
        # print(data["main"])

        df = pd.DataFrame.from_records(data["main"], index=[0])
        # print(df.head())

        dataset = PandasDataset(df)
        # print(dataset.head())

        results = dataset.expect_column_values_to_be_between(
            column="temp",
            min_value=20,
            max_value=40,
        )
        # print(results)
        assert results["success"] is True


def _create_weather_table(**context):
    pg_hook = PostgresHook(
        postgres_conn_id="my_postgres_conn",
        schema="postgres"
    )
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    sql = """
        CREATE TABLE IF NOT EXISTS weathers (
            dt BIGINT NOT NULL,
            temp FLOAT NOT NULL
        )
    """
    cursor.execute(sql)
    connection.commit()


def _load_data_to_postgres(**context):
    ti = context["ti"]
    file_name = ti.xcom_pull(task_ids="get_weather_data", key="return_value")
    print(file_name)

    pg_hook = PostgresHook(
        postgres_conn_id="my_postgres_conn",
        schema="postgres"
    )
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    with open(file_name, "r") as f:
        data = json.load(f)

    temp = data["main"]["temp"]
    dt = data["dt"]
    sql = f"""
        INSERT INTO weathers (dt, temp) VALUES ({dt}, {temp})
    """
    cursor.execute(sql)
    connection.commit()


default_args = {
    "email": ["kan@odds.team"],
    # "retries": 1,
}
with DAG(
    "weather_api_dag",
    default_args=default_args,
    schedule="@hourly",
    start_date=timezone.datetime(2024, 2, 3),
    catchup=False,
):
    start = EmptyOperator(task_id="start")

    get_weather_data = PythonOperator(
        task_id="get_weather_data",
        python_callable=_get_weather_data,
    )

    validate_temperature = PythonOperator(
        task_id="validate_temperature",
        python_callable=_validate_temperature,
    )

    create_weather_table = PythonOperator(
        task_id="create_weather_table",
        python_callable=_create_weather_table,
    )

    load_data_to_postgres = PythonOperator(
        task_id="load_data_to_postgres",
        python_callable=_load_data_to_postgres,
    )

    end = EmptyOperator(task_id="end")

    start >> get_weather_data >> validate_temperature >> create_weather_table >> load_data_to_postgres >> end
