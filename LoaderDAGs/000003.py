from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator
from airflow.providers.jdbc.operators.jdbc import JdbcOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime as dt

args = {
    'owner': 'Seshu Edala',
}

with DAG(
    dag_id='artist_external_to_artist',
    default_args=args,
    schedule_interval='*/30 * * * *',
    dagrun_timeout=timedelta(minutes=5),
    start_date=days_ago(1),
    tags=['artist_external_to_artist', 'load_data', 'aluminium'],
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
drop view if exists global_temp.artist_{__signature__};

-- exclude previously processed files
create global temporary view artist_{__signature__} as 
  select distinct input_file_name() filename, "music.artist_external" al_table
    from music.artist_external minus
    (select distinct data_file filename, "music.artist_external" al_table from meta.control where al_table = "music.artist_external");

-- process new data
insert into music.artist (`id`, `record_label_id`, `name`) select `id`, `record_label_id`, `name`
    from (select /*+ mapjoin(b) */ cast(`id` as integer) `id`, cast(`record_label_id` as integer) `record_label_id`, cast(`name` as string) `name`, input_file_name() filename FROM music.artist_external) a
    join global_temp.artist_{__signature__} b on a.filename=b.filename;

-- waterline data
insert into meta.control 
  select distinct filename data_file, al_table al_table, current_timestamp process_time from global_temp.artist_{__signature__};

-- drop temporary table
drop view if exists global_temp.artist_{__signature__};
'''
        __signature__= str(abs(hash('artist_external_to_artist')))
        task_create_database_if_not_exists_meta_0 = JdbcOperator(sql=f'''CREATE DATABASE IF NOT EXISTS meta;''', jdbc_conn_id='airflow_conn_id', task_id="task_create_database_if_not_exists_meta_0", dag=dag,)
        task_create_external_table_if_not_exists_meta_control_data_file_strin_1 = JdbcOperator(sql=f'''CREATE EXTERNAL TABLE IF NOT EXISTS meta.control (data_file string, al_table string, process_time TIMESTAMP) USING delta LOCATION "s3a://spark/warehouse/control";''', jdbc_conn_id='airflow_conn_id', task_id="task_create_external_table_if_not_exists_meta_control_data_file_strin_1", dag=dag,)
        task_use_music_2 = JdbcOperator(sql=f'''USE music;''', jdbc_conn_id='airflow_conn_id', task_id="task_use_music_2", dag=dag,)
        task_drop_view_if_exists_global_temp_artist_signature_3 = JdbcOperator(sql=f'''DROP VIEW IF EXISTS global_temp.artist_{__signature__};''', jdbc_conn_id='airflow_conn_id', task_id="task_drop_view_if_exists_global_temp_artist_signature_3", dag=dag,)
        task_create_global_temporary_view_artist_signature_as_select_distinct_4 = JdbcOperator(sql=f'''CREATE GLOBAL TEMPORARY VIEW artist_{__signature__} AS SELECT DISTINCT input_file_name() filename, "music.artist_external" al_table FROM music.artist_external MINUS (SELECT DISTINCT data_file filename, "music.artist_external" al_table FROM meta.control WHERE al_table = "music.artist_external");''', jdbc_conn_id='airflow_conn_id', task_id="task_create_global_temporary_view_artist_signature_as_select_distinct_4", dag=dag,)
        task_insert_into_music_artist_id_record_label_id_name_select_id_recor_5 = JdbcOperator(sql=f'''INSERT INTO music.artist (`id`, `record_label_id`, `name`) SELECT `id`, `record_label_id`, `name` FROM (SELECT cast(`id` AS integer) `id`, cast(`record_label_id` AS integer) `record_label_id`, cast(`name` AS string) `name`, input_file_name() filename FROM music.artist_external) a JOIN global_temp.artist_{__signature__} b ON a.filename = b.filename;''', jdbc_conn_id='airflow_conn_id', task_id="task_insert_into_music_artist_id_record_label_id_name_select_id_recor_5", dag=dag,)
        task_insert_into_meta_control_select_distinct_filename_data_file_al_t_6 = JdbcOperator(sql=f'''INSERT INTO meta.control SELECT DISTINCT filename data_file, al_table al_table, CURRENT_TIMESTAMP process_time FROM global_temp.artist_{__signature__};''', jdbc_conn_id='airflow_conn_id', task_id="task_insert_into_meta_control_select_distinct_filename_data_file_al_t_6", dag=dag,)
        task_drop_view_if_exists_global_temp_artist_signature_7 = JdbcOperator(sql=f'''DROP VIEW IF EXISTS global_temp.artist_{__signature__};''', jdbc_conn_id='airflow_conn_id', task_id="task_drop_view_if_exists_global_temp_artist_signature_7", dag=dag,)
        
        task_create_database_if_not_exists_meta_0 >> task_create_external_table_if_not_exists_meta_control_data_file_strin_1 >> task_use_music_2 >> task_drop_view_if_exists_global_temp_artist_signature_3 >> task_create_global_temporary_view_artist_signature_as_select_distinct_4 >> task_insert_into_music_artist_id_record_label_id_name_select_id_recor_5 >> task_insert_into_meta_control_select_distinct_filename_data_file_al_t_6 >> task_drop_view_if_exists_global_temp_artist_signature_7