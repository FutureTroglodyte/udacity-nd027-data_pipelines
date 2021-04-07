from datetime import datetime

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator


dag = DAG("delete_tables", start_date=datetime.now(), schedule_interval="@once")

delete_tables = PostgresOperator(
    task_id="delete_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql="""
    	DROP TABLE IF EXISTS public.artists;

		DROP TABLE IF EXISTS public.songplays;

		DROP TABLE IF EXISTS public.songs;

		DROP TABLE IF EXISTS public.staging_events;

		DROP TABLE IF EXISTS public.staging_songs;

		DROP TABLE IF EXISTS public."time";

		DROP TABLE IF EXISTS public.users;
    """,
)
