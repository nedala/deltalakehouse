
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
    tags=(['ESC_EDW_ETL_DAG_02', 'etl'] + ['ticker', 'select']),
    catchup=False,
    
) as dag:
                task_sql_queries_sql04_sql_4_4 = JdbcOperator(sql=f'''SELECT * FROM ticker LIMIT 100;''', jdbc_conn_id='airflow_conn_id', task_id="task_sql_queries_sql04_sql_4_4", dag=dag,)
                task_end = JdbcOperator(sql=f'''-- FINISHED EXECUTION;''', jdbc_conn_id='airflow_conn_id', task_id="task_end", dag=dag,)
                task_ticker = JdbcOperator(sql=f'''SELECT 1; -- FIXME TICKER;''', jdbc_conn_id='airflow_conn_id', task_id="task_ticker", dag=dag,)
                
                task_end.set_upstream(task_sql_queries_sql04_sql_4_4)
                task_sql_queries_sql04_sql_4_4.set_upstream(task_ticker)
