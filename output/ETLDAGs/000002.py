
from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator
from airflow.providers.jdbc.operators.jdbc import JdbcOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime as dt

args = {
    'owner': 'Seshu Edala',
}

with DAG(
    dag_id='ESC_EDW_ETL_DAG_02',
    default_args=args,
    schedule_interval='@daily',
    dagrun_timeout=timedelta(minutes=60),
    start_date=days_ago(1),
    tags=(['ESC_EDW_ETL_DAG_02', 'etl'] + ['table', 'miller', 'update', 'robert']),
    catchup=False,
    
) as dag:
                task_table = JdbcOperator(sql=f'''SELECT 1; -- FIXME TABLE;''', jdbc_conn_id='airflow_conn_id', task_id="task_table", dag=dag,)
                task_end = JdbcOperator(sql=f'''-- FINISHED EXECUTION;''', jdbc_conn_id='airflow_conn_id', task_id="task_end", dag=dag,)
                task_sql_queries_sql02_sql_1_15 = JdbcOperator(sql=f'''UPDATE TABLE album SET YEAR = 1981 WHERE id = 1;''', jdbc_conn_id='airflow_conn_id', task_id="task_sql_queries_sql02_sql_1_15", dag=dag,)
                
                task_end.set_upstream(task_table)
                task_table.set_upstream(task_sql_queries_sql02_sql_1_15)
