from airflow import DAG
from operators.ssh_spark import CustomSSHSparkOperator
from airflow.utils.dates import days_ago
from airflow.decorators import task_group, dag

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

@dag(
    dag_id="ETL-test",
    catchup=False,
    tags=["Spark", "SSH", "Iceberg"],
    default_args=default_args
)

def ETL_test_dag():
    #########################################
    #   TASK GROUP: BATCH DATA INGESTION    #
    #########################################
    @task_group(group_id='batch-ingestion-layer',
                tooltip="This task group ingests data from MongoDB (bank production database) into the bronz layer",
                prefix_group_id=True,
    )
    def batch_ingestion_task_group():
        ssh_spark_task1 = CustomSSHSparkOperator(
            task_id='mongoDB-Fraud-Transactions',
            ssh_conn_id='my_ssh_connection',
            application_path='/includes/ingestion/mongoDB/fraud.py',
		)
        
        ssh_spark_task2 = CustomSSHSparkOperator(
            task_id='mongoDB-Customers',
            ssh_conn_id='my_ssh_connection',
            application_path='/includes/ingestion/mongoDB/clients.py',
		)
        [ssh_spark_task1, ssh_spark_task2]
        
    ############################################
    #   TASK GROUP: CLEANING AND ENRICHMENT    #
    ############################################
    @task_group(group_id='data-cleaning-and-enrichment')
    def data_cleaning_and_enrichment():
        ssh_spark_task1 = CustomSSHSparkOperator(
            task_id='transactions',
            ssh_conn_id='my_ssh_connection',
            application_path='/includes/python_scripts/data_operations/silver/transactions.py',
		)
        
        ssh_spark_task2 = CustomSSHSparkOperator(
            task_id='customers',
            ssh_conn_id='my_ssh_connection',
            application_path='/includes/python_scripts/data_operations/silver/customers.py',
		)
        
        ssh_spark_task3 = CustomSSHSparkOperator(
            task_id='fraud',
            ssh_conn_id='my_ssh_connection',
            application_path='/includes/python_scripts/data_operations/silver/fraud.py',
		)
        
        [ssh_spark_task1, ssh_spark_task2, ssh_spark_task3]
        
    ###################################
    #   TASK GROUP: GOLD VIEWS ..     #
    ###################################
    # @task_group(group_id='creating-gold-views')
    # def creating_gold_views():
    #     ssh_dremio_task = CustomSSHSparkOperator(
    #         task_id='views',
    #         ssh_conn_id='my_ssh_connection',
    #         application_path='/includes/python_scripts/data_operations/gold/gold_views.py',
	# 	)
    #     ssh_dremio_task

    batch_ingestion_task_group() >> data_cleaning_and_enrichment() #>> creating_gold_views()
ETL_test_dag()