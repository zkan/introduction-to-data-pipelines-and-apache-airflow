from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone


def _get_data():
    pg_hook = PostgresHook(
        postgres_conn_id="my_postgres_conn",
        schema="postgres"
    )
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    sql = """
        select * from information_schema.tables
    """
    cursor.execute(sql)
    rows = cursor.fetchall()
    for each in rows:
        print(each)


# def _dump_data(table: str):
#     pg_hook = PostgresHook(
#         postgres_conn_id="my_postgres_conn",
#         schema="postgres"
#     )
#     pg_hook.bulk_dump(table, f"/opt/airflow/dags/{table}_export")
    

with DAG(
    dag_id="play_with_airflow_connections_and_hooks",
    schedule="@daily",
    start_date=timezone.datetime(2024, 1, 20),
    catchup=False,
):

    get_data = PythonOperator(
        task_id="get_data",
        python_callable=_get_data,
    )

    # dump_product_data = PythonOperator(
    #     task_id="dump_weathers_data",
    #     python_callable=_dump_data,
    #     op_kwargs={"table": "weathers"},
    # )