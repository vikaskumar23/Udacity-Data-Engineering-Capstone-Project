import datetime
import logging
import requests
from airflow.models import Variable
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.ssh_operator import SSHOperator

from airflow.operators import (
    HasRowsOperator,
    PostgresOperator,
    PythonOperator,
    S3ToRedshiftOperator,
    RedshiftToS3Operator
)

import sql_statements



elastic = Variable.get("elastic")
bucket = Variable.get("bucket")


def check_index_data(**kwargs):

    """ Check data in elasticsearch Index"""
    
    response=requests.get(f"http://{kwargs['base_url']}:9200/{kwargs['index']}/_count")
    count=response.json()['count']
    if count < 1:
        raise ValueError(f"Data quality check failed. {kwargs['index']} contained 0 documents")
    logging.info(f"Data quality on index {kwargs['index']} check passed with {count} records")

# DAG Initialisation
dag = DAG(
    "capstone-project",
    start_date=datetime.datetime.now(),
    schedule_interval="@monthly",
    max_active_runs=1
)

start = DummyOperator(
    task_id='begin_execution',
    dag=dag,
)

# Spark Job to Clean Immigration Data
data_cleaning = SSHOperator(
    task_id='data_cleaning_spark_job',
    dag=dag,
    ssh_conn_id='my_ssh_connection',
    command='/usr/bin/spark-submit --jars "spark-sas7bdat-2.1.0-s_2.11.jar,parso-2.0.10.jar" --master yarn clean_job.py',
)

# Create Immigration Fact Table
create_immigration_table = PostgresOperator(
    task_id="create_immigration_fact_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_IMMIGRATION_FACT_SQL
)

# Create Happiness Table
create_happiness_table = PostgresOperator(
    task_id="create_happiness_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_HAPPINESS_SQL
)

# Create Country Table
create_country_table = PostgresOperator(
    task_id="create_country_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_COUNTRY_SQL
)

# Create Happiness Dimension Table
create_happiness_dim_table = PostgresOperator(
    task_id="create_happiness_dim_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_HAPPINESS_DIMENSION_SQL
)

# Load Happiness data to Table
load_happiness_dim_table = PostgresOperator(
    task_id="load_happiness_dim_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.LOAD_HAPPINESS_DIM_SQL
)

# Create Mode Dimension Table
create_mode_dim_table = PostgresOperator(
    task_id="create_mode_dim_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_MODE_SQL
)

# Create Visa Dimension Table
create_visa_dim_table = PostgresOperator(
    task_id="create_visa_dim_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_VISA_SQL
)

# load immigration data to immigration_fact table
copy_immigration_task = S3ToRedshiftOperator(
    task_id="load_immigration_from_s3_to_redshift",
    dag=dag,
    table="immigration_fact",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket=bucket,
    s3_key="immigration-data/cleanjob.csv"
)

# Load happiness data to happiness table
copy_happiness_task = S3ToRedshiftOperator(
    task_id="load_happiness_from_s3_to_redshift",
    dag=dag,
    table="happiness",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket=bucket,
    s3_key="happiness-data/2016.csv"
)

# Load country data to Country table
copy_country_task = S3ToRedshiftOperator(
    task_id="load_country_from_s3_to_redshift",
    dag=dag,
    table="country",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket=bucket,
    s3_key="country.csv"
)

# Load mode data to Mode Dimension table
copy_mode_task = S3ToRedshiftOperator(
    task_id="load_mode_from_s3_to_redshift",
    dag=dag,
    table="mode_dim",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket=bucket,
    s3_key="mode.csv"
)

# Load Visa data to Visa Dimension table
copy_visa_task = S3ToRedshiftOperator(
    task_id="load_visa_from_s3_to_redshift",
    dag=dag,
    table="visa_dim",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket=bucket,
    s3_key="visa.csv"
)

# Check Rows count in immigration table
check_immigration = HasRowsOperator(
    task_id='check_immigration_data',
    dag=dag,
    table='immigration_fact',
    redshift_conn_id='redshift'
)

# Check Rows count in Happiness table 
check_happiness = HasRowsOperator(
    task_id='check_happiness_data',
    dag=dag,
    table='happiness',
    redshift_conn_id='redshift'
)

# Check Rows count in Country table
check_country = HasRowsOperator(
    task_id='check_country_data',
    dag=dag,
    table='country',
    redshift_conn_id='redshift'
)

# Check Rows count in Mode table
check_mode = HasRowsOperator(
    task_id='check_mode_data',
    dag=dag,
    table='mode_dim',
    redshift_conn_id='redshift'
)

# Check Rows count in Mode table
check_visa = HasRowsOperator(
    task_id='check_visa_data',
    dag=dag,
    table='visa_dim',
    redshift_conn_id='redshift'
)

# Check Rows count in Happiness Dimension table
check_happiness_dim = HasRowsOperator(
    task_id='check_happiness_dim_data',
    dag=dag,
    table='happiness_dim',
    redshift_conn_id='redshift'
)

# Create Result table by combining all the fact and Dimension Tables
analytics = PostgresOperator(
    task_id='do_analytics',
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_RESULT_SQL
)

# Move table Result data to S3
save_result_to_s3 = RedshiftToS3Operator(
    task_id='save_result_to_s3',
    dag=dag,
    table='result',
    s3_bucket=bucket,
    s3_key='analytic-results/result.csv',
    aws_credentials_id='aws_credentials',
    redshift_conn_id='redshift'
)

# Check Rows count in Result Dimension table
check_result = HasRowsOperator(
    task_id='check_result_data',
    dag=dag,
    table='result',
    redshift_conn_id='redshift'
)

# Spark Job to fetch data from Redhsift and Push to Elastic
move_data_to_elastic = SSHOperator(
    task_id='move_data_to_elastic_spark_job',
    dag=dag,
    ssh_conn_id='my_ssh_connection',
    command=f'/usr/bin/spark-submit --jars "elasticsearch-hadoop-7.8.2-20200817.003026-37.jar,postgresql-42.2.14.jar" --master yarn elastic_job.py {elastic}',
)

# Check docs count in Elastic
check_elastic_data = PythonOperator(
    task_id='check_elastic_data',
    dag=dag,
    python_callable=check_index_data,
    provide_context=True,
    op_kwargs={
        'index': 'immigration-',
        'base_url' : elastic
        },
)

end = DummyOperator(
    task_id='end_execution',
    dag=dag,
)

start >> data_cleaning
data_cleaning >> [create_immigration_table, create_happiness_table, create_country_table, create_mode_dim_table,
                  create_visa_dim_table]
create_immigration_table >> copy_immigration_task >> check_immigration
create_happiness_table >> copy_happiness_task >> check_happiness >> create_happiness_dim_table
create_country_table >> copy_country_task >> check_country >> create_happiness_dim_table
create_happiness_dim_table >> load_happiness_dim_table >> check_happiness_dim
create_mode_dim_table >> copy_mode_task >> check_mode
create_visa_dim_table >> copy_visa_task >> check_visa
[check_immigration, check_happiness_dim, check_mode, check_visa] >> analytics >> check_result
check_result >> move_data_to_elastic >> check_elastic_data >> end
check_result >> save_result_to_s3 >> end
