from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,
)
from helpers import SqlQueries


default_args = {
    "owner": "udacity",
    "owner": "Sparkify",
    # "start_date": datetime(2019, 1, 12),
    "start_date": datetime.now(),
    "depends_on_past": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=10),
    "catchup_by_default": False,
}

dag = DAG(
    "sparkify_data_pipeline",
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval="0 * * * *",
    # schedule_interval="@once",
)

start_operator = DummyOperator(task_id="Begin_execution", dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id="Stage_events",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log-data",
    jsonpath="s3://udacity-dend/log_json_path.json",
    delimiter=",",
    ignore_headers=1,
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="Stage_songs",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    jsonpath="",
    delimiter=",",
    ignore_headers=1,
)

load_songplays_table = LoadFactOperator(
    task_id="Load_songplays_fact_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    select_statement=SqlQueries.songplay_table_insert,
    if_exists="replace",
)

load_user_dimension_table = LoadDimensionOperator(
    task_id="Load_user_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    select_statement=SqlQueries.user_table_insert,
    if_exists="replace",
)

load_song_dimension_table = LoadDimensionOperator(
    task_id="Load_song_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    select_statement=SqlQueries.song_table_insert,
    if_exists="replace",
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id="Load_artist_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    select_statement=SqlQueries.artist_table_insert,
    if_exists="replace",
)

load_time_dimension_table = LoadDimensionOperator(
    task_id="Load_time_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    select_statement=SqlQueries.time_table_insert,
    if_exists="replace",
)

run_quality_checks = DataQualityOperator(
    task_id="Run_data_quality_checks",
    dag=dag,
    redshift_conn_id="redshift",
    tables_list=["songplays", "users", "songs", "artists", "time"],
    checks_list=["emptyness", "duplicates", "missing_values"],
)

end_operator = DummyOperator(task_id="Stop_execution", dag=dag)


# Set Task Dependencies

start_operator >> [
    stage_events_to_redshift,
    stage_songs_to_redshift,
] >> load_songplays_table
load_songplays_table >> [
    load_user_dimension_table,
    load_song_dimension_table,
    load_artist_dimension_table,
    load_time_dimension_table,
] >> run_quality_checks >> end_operator
