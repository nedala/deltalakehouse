from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator
from airflow.providers.jdbc.operators.jdbc import JdbcOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime as dt

args = {
    'owner': 'Seshu Edala',
}

with DAG(
    dag_id='album_external_to_album',
    default_args=args,
    schedule_interval='*/30 * * * *',
    dagrun_timeout=timedelta(minutes=5),
    start_date=days_ago(1),
    tags=['album_external_to_album', 'load_data', 'aluminium'],
    catchup=False,
    
) as dag:
        '''
-- create control table
create database if not exists meta;
create external table if not exists meta.control 
  (data_file string, al_table string, process_time timestamp) 
  using delta location "s3a://spark/warehouse/control";

-- switch to correct database
use music;

-- drop previous table
drop view if exists global_temp.album_{__signature__};

-- exclude previously processed files
create global temporary view album_{__signature__} as 
  select distinct input_file_name() filename, "music.album_external" al_table
    from music.album_external minus
    (select distinct data_file filename, "music.album_external" al_table from meta.control where al_table = "music.album_external");

-- process new data
insert into music.album (`id`, `artist_id`, `name`, `year`) select `id`, `artist_id`, `name`, `year`
    from (select /*+ mapjoin(b) */ cast(`id` as integer) `id`, cast(`artist_id` as integer) `artist_id`, cast(`name` as string) `name`, cast(`year` as integer) `year`, input_file_name() filename FROM music.album_external) a
    join global_temp.album_{__signature__} b on a.filename=b.filename;

-- waterline data
insert into meta.control 
  select distinct filename data_file, al_table al_table, current_timestamp process_time from global_temp.album_{__signature__};

-- drop temporary table
drop view if exists global_temp.album_{__signature__};
'''
        __signature__= str(abs(hash('album_external_to_album')))
        task_create_database_if_not_exists_meta_0 = JdbcOperator(sql=f'''CREATE DATABASE IF NOT EXISTS meta;''', jdbc_conn_id='airflow_conn_id', task_id="task_create_database_if_not_exists_meta_0", dag=dag,)
        task_create_external_table_if_not_exists_meta_control_data_file_strin_1 = JdbcOperator(sql=f'''CREATE EXTERNAL TABLE IF NOT EXISTS meta.control (data_file string, al_table string, process_time TIMESTAMP) USING delta LOCATION "s3a://spark/warehouse/control";''', jdbc_conn_id='airflow_conn_id', task_id="task_create_external_table_if_not_exists_meta_control_data_file_strin_1", dag=dag,)
        task_use_music_2 = JdbcOperator(sql=f'''USE music;''', jdbc_conn_id='airflow_conn_id', task_id="task_use_music_2", dag=dag,)
        task_drop_view_if_exists_global_temp_album_signature_3 = JdbcOperator(sql=f'''DROP VIEW IF EXISTS global_temp.album_{__signature__};''', jdbc_conn_id='airflow_conn_id', task_id="task_drop_view_if_exists_global_temp_album_signature_3", dag=dag,)
        task_create_global_temporary_view_album_signature_as_select_distinct__4 = JdbcOperator(sql=f'''CREATE GLOBAL TEMPORARY VIEW album_{__signature__} AS SELECT DISTINCT input_file_name() filename, "music.album_external" al_table FROM music.album_external MINUS (SELECT DISTINCT data_file filename, "music.album_external" al_table FROM meta.control WHERE al_table = "music.album_external");''', jdbc_conn_id='airflow_conn_id', task_id="task_create_global_temporary_view_album_signature_as_select_distinct__4", dag=dag,)
        task_insert_into_music_album_id_artist_id_name_year_select_id_artist__5 = JdbcOperator(sql=f'''INSERT INTO music.album (`id`, `artist_id`, `name`, `year`) SELECT `id`, `artist_id`, `name`, `year` FROM (SELECT cast(`id` AS integer) `id`, cast(`artist_id` AS integer) `artist_id`, cast(`name` AS string) `name`, cast(`year` AS integer) `year`, input_file_name() filename FROM music.album_external) a JOIN global_temp.album_{__signature__} b ON a.filename = b.filename;''', jdbc_conn_id='airflow_conn_id', task_id="task_insert_into_music_album_id_artist_id_name_year_select_id_artist__5", dag=dag,)
        task_insert_into_meta_control_select_distinct_filename_data_file_al_t_6 = JdbcOperator(sql=f'''INSERT INTO meta.control SELECT DISTINCT filename data_file, al_table al_table, CURRENT_TIMESTAMP process_time FROM global_temp.album_{__signature__};''', jdbc_conn_id='airflow_conn_id', task_id="task_insert_into_meta_control_select_distinct_filename_data_file_al_t_6", dag=dag,)
        task_drop_view_if_exists_global_temp_album_signature_7 = JdbcOperator(sql=f'''DROP VIEW IF EXISTS global_temp.album_{__signature__};''', jdbc_conn_id='airflow_conn_id', task_id="task_drop_view_if_exists_global_temp_album_signature_7", dag=dag,)
        
        task_create_database_if_not_exists_meta_0 >> task_create_external_table_if_not_exists_meta_control_data_file_strin_1 >> task_use_music_2 >> task_drop_view_if_exists_global_temp_album_signature_3 >> task_create_global_temporary_view_album_signature_as_select_distinct__4 >> task_insert_into_music_album_id_artist_id_name_year_select_id_artist__5 >> task_insert_into_meta_control_select_distinct_filename_data_file_al_t_6 >> task_drop_view_if_exists_global_temp_album_signature_7