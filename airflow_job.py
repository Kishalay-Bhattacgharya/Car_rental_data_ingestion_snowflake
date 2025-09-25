from airflow import DAG
from datetime import timedelta,datetime
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator


def get_execution_date(**kwargs):
    execution_date=kwargs['params'].get('execution_date')

    if execution_date =='NA':
        execution_date=kwargs.get('ds_nodash')

    return execution_date    



with DAG(
    dag_id='Load_Customers_rentals_data',
    start_date=datetime(2025,9,23),
    schedule_interval=None,
    description='To load of customers SCD -2 type and load fact_rentals data into snowflake',
    tags=['Snowflake','Spark'],
    catchup=False,
    params={
        'execution_date':Param(default='NA',type='string')
    }
    
) as dag:
    
    execution_date=PythonOperator(
        task_id='Get_Execution_date_from_dag',
        python_callable=get_execution_date,
        dag=dag
    )

    #To perform SCD-2 type operation on dim_customer table from airflow, only pulling the file as per date passed from airflow parameter..
    update_customers=SQLExecuteQueryOperator(
        task_id='Update_customers_table',
        sql='''
        Merge into dim_customer as target 
        using 
        (
        select $1 as customer_id,
        $2 as customer_name ,
        $3 as Email,
        $4 as number
        from @Rentals/customers_{{ti.xcom_pull(task_ids='Get_Execution_date_from_dag')}}.csv 
        (FILE_FORMAT => 'csv_format')
        ) as source

        on Source.Customer_id=Target.Customer_id and Target.is_current=True

        when matched and (source.customer_name != target.name or source.email !=target.email or source.number!=target.phone)
        then update 
        set target.end_date=current_timestamp() , target.is_current=False;

        '''
       ,conn_id='snowflake_conn_id_spark',dag=dag
    
    )

    insert_customers=SQLExecuteQueryOperator(
        task_id='Insert_new_customers',
        sql='''INSERT INTO dim_customer (customer_id, name, email, phone, effective_date, end_date, is_current)
        SELECT
            $1 AS customer_id,
            $2 AS name,
            $3 AS email,
            $4 AS phone,
            CURRENT_TIMESTAMP() AS effective_date,
            NULL AS end_date,
            TRUE AS is_current
        FROM @Rentals/customers_{{ ti.xcom_pull(task_ids='Get_Execution_date_from_dag') }}.csv (FILE_FORMAT => 'csv_format');


        ''',dag=dag,conn_id='snowflake_conn_id_spark'
    )



    pyspark_job={
        'placement':{
            'cluster_name':'democluster'
        },
        'pyspark_job':{
            'main_python_file_uri':'gs://snowflake_projects_kishalay/jobs/spark_job.py',
            'args':["--date={{ ti.xcom_pull(task_ids='Get_Execution_date_from_dag') }}"],
            'jar_file_uris':['gs://snowflake_projects_kishalay/jars/snowflake-jdbc-3.26.1.jar','gs://snowflake_projects_kishalay/jars/spark-snowflake_2.12-3.1.4.jar']
        }
    }

    #To run the spark job which joins all the dimension tables like dim_date,dim_customer,dim_location,dim_car and fact_rentals 
    # and populate the fact_rentals table.
    run_spark_job=DataprocSubmitJobOperator(
        task_id='Spark_To_Load_Car_Rentals',
        project_id='credible-list-438607-n8',
        region='us-central1',
        job=pyspark_job,dag=dag

    )

    
    execution_date >>update_customers >> insert_customers >> run_spark_job


