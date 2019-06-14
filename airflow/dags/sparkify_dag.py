from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 14),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup' : False
}

dag = DAG('sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval=None
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

s3_key = "log_data"
s3_bucket = "udacity-dend"
redshift_conn = "redshift"
staging_table = "staging_events"
aws_credentials= "aws_credentials"
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table=staging_table,
    redshift_conn_id=redshift_conn,
    aws_credentials_id=aws_credentials,
    s3_bucket=s3_bucket,
    jsonPath = "log_json_path.json",
    s3_key=s3_key,
    format= "json"
)
staging_songs = "staging_songs"
s3_key = "song_data"
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table=staging_songs,
    redshift_conn_id=redshift_conn,
    aws_credentials_id=aws_credentials,
    s3_bucket=s3_bucket,
    jsonPath = "auto",
    s3_key=s3_key,
    format= "json"
)
fact_table = "songplays"
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table=fact_table,
    sql_query=SqlQueries.songplay_table_insert,
    redshift_conn_id=redshift_conn
)

dim_user_table= "users"
append = False
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    append=append,
    table=dim_user_table,
    sql_query=SqlQueries.user_table_insert,
    redshift_conn_id=redshift_conn
)

dim_song_table="songs"
append = False
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    append=append,
    table=dim_song_table,
    sql_query=SqlQueries.song_table_insert,
    redshift_conn_id=redshift_conn    
)

dim_artist_table="artists"
append = False
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    append=append,
    table=dim_artist_table,
    sql_query=SqlQueries.artist_table_insert,
    redshift_conn_id=redshift_conn    
)

dim_time_table= "time"
append = False
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    append=append,
    table=dim_time_table,
    sql_query=SqlQueries.time_table_insert,
    redshift_conn_id=redshift_conn     
)
tableList =[dim_user_table,dim_song_table,dim_artist_table,dim_time_table]
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id=redshift_conn,
    tableList=tableList
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator