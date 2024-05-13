
from airflow import DAG 
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from fivetran_provider_async.operators import FivetranOperator
from fivetran_provider_async.sensors import FivetranSensor
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime, timedelta
from var.variabales import *

default_args={
    "owner":"ct",
    "email_on_failer":True,
    "email_on_retry":False,
    "email":"mm22@gmail.com",
    "retries":1,
    "retry_delay":timedelta(minutes=5)
}

with DAG(
    "customer_transactions_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 2 * * FRI",
    default_args=default_args,
    catchup=False,
    default_view="graph"
) as dag:
    
    Start=EmptyOperator(task_id="Start")
    
    End=EmptyOperator(task_id="End")
    
    with TaskGroup(group_id="Ingestion_Fivetran") as Ingestion_Fivetran:
        
        FivetranOperator(
            task_id="MongoDB_to_AzureStorage",
            fivetran_conn_id="fivetran_default",
            connector_id=Mongodb_connection_id,
            schedule_type="manual"
        )
        
        FivetranSensor(
            task_id="wait_for_ingestion",
            fivetran_conn_id="fivetran_default",
            connector_id=Mongodb_connection_id,
            poke_interval=30
        )
        
    with TaskGroup(group_id="Set_configs_Databricks") as set_configs_Databricks:
        
        DatabricksRunNowOperator(
            task_id="mount_adfs_with_dbfs",
            databricks_conn_id="databricks_default",
            job_id=config_Job_id
        )
    
    with TaskGroup(group_id="Create_Process_schema_Spark_SQL") as Create_Process_schema_Spark_SQL:
        
        DatabricksRunNowOperator(
            task_id="DDL_process_schema",
            databricks_conn_id="databricks_default",
            job_id=create_process_schema_SQL_job_id
        )
        
    with TaskGroup(group_id="Process_layer_PySpark") as Process_layer_PySpark:
        
        DatabricksRunNowOperator(
            task_id="customer_processing",
            databricks_conn_id="databricks_default",
            job_id=customers_processing_job_id
        )
        
        DatabricksRunNowOperator(
            task_id="account_processing",
            databricks_conn_id="databricks_default",
            job_id=accounts_processing_job_id
        )
        
        DatabricksRunNowOperator(
            task_id="transactions_processing",
            databricks_conn_id="databricks_default",
            job_id=transactions_processing_job_id
        )
        
    with TaskGroup(group_id="Create_presentation_schema_spark_SQL") as Create_presentation_schema_spark_SQL:
        
        DatabricksRunNowOperator(
            task_id="DDL_presentation_schema",
            databricks_conn_id="databricks_default",
            job_id=create_presentation_schema_SQL_job_id
        )
        
    with TaskGroup(group_id="Presentation_layer_PySpark") as Presentation_layer_PySpark:
        
        dim_customer_transformation=DatabricksRunNowOperator(
            task_id="dim_customer_transformation",
            databricks_conn_id="databricks_default",
            job_id=dim_customer_transformation_job_id
        )
        
        dim_account_transformation=DatabricksRunNowOperator(
            task_id="dim_account_transformation",
            databricks_conn_id="databricks_default",
            job_id=dim_account_transformation_job_id
        )
        
        dim_transaction_details_transformation=DatabricksRunNowOperator(
            task_id="dim_transaction_details_transformation",
            databricks_conn_id="databricks_default",
            job_id=dim_transaction_details_transformation_job_id
        )
        
        fact_transactions_transformation=DatabricksRunNowOperator(
            task_id="fact_transactions_transformation",
            databricks_conn_id="databricks_default",
            job_id=fact_transaction_transformation_job_id
        )
        
        dim_customer_transformation >> fact_transactions_transformation
        dim_account_transformation >> fact_transactions_transformation
        dim_transaction_details_transformation >> fact_transactions_transformation
        
    
    Start >> Ingestion_Fivetran >> set_configs_Databricks >> Create_Process_schema_Spark_SQL
    Create_Process_schema_Spark_SQL >> Process_layer_PySpark >> Create_presentation_schema_spark_SQL
    Create_presentation_schema_spark_SQL >> Presentation_layer_PySpark >> End