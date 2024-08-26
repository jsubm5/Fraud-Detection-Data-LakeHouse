from airflow import DAG # type: ignore
from operators.ssh_spark import CustomSSHSparkOperator
from airflow.utils.dates import days_ago
from airflow.decorators import task, dag
from airflow.providers.ssh.operators.ssh import SSHOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

@dag(
    dag_id="SSH-test",
    catchup=False,
    tags=["Spark", "SSH"],
    default_args=default_args
)

def test_ssh():
    test=SSHOperator(
        task_id='test',
        ssh_conn_id='my_ssh_connection',
        cmd_timeout=None,
        command="""
        source /etc/profile
        cat /etc/profile
        echo $AWS_ACCESS_KEY_ID
        echo $AWS_SECRET_ACCESS_KEY
        echo $AWS_REGION
        echo $AWS_DEFAULT_REGION
        """
    )
    test
    
test_ssh()