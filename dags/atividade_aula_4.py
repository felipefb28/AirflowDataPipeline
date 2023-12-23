from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from  airflow.utils.dates import days_ago

with DAG(
    'my_dag_aula_4',
    start_date=days_ago(1),
    schedule_interval='@daily'
) as dag:
    def cumprimentos():
        print("Boas vindas ao Airflow!")
        
    task1 = PythonOperator(
        task_id = 'tarefa_unica',
        python_callable = cumprimentos
    )