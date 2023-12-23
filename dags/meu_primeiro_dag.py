from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash_operator import BashOperator


with DAG(
    'my_first_dag',
    start_date=days_ago(2),
    schedule_interval='@daily'
) as dag:
    task1=EmptyOperator(task_id='tarefa1')
    task2=EmptyOperator(task_id='tarefa2')
    task3=EmptyOperator(task_id='tarefa3')
    task4=BashOperator(
        task_id='tarefa4',
        bash_command= 'mkdir -p "/home/felipeflaviobento/Área de Trabalho/ALuraProjects/AirflowDataPipeline/pasta={{data_interval_end}}"'
    )

    task1 >> [task2,task3]
    task3 >> task4