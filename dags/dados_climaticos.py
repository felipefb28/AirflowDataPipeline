import pendulum
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import os
import pandas as pd
from airflow.macros import ds_add

with DAG(
    'dag_dados_climaticos',
    start_date=pendulum.datetime(2023, 11, 27, tz="UTC"),
    schedule_interval='0 0 * * 1'

) as dag:
    
    task1 = BashOperator(
        task_id = 'cria_pasta',
        bash_command = 'mkdir -p "/home/felipeflaviobento/Área de Trabalho/ALuraProjects/AirflowDataPipeline/semana={{data_interval_end.strftime("%Y-%m-%d")}}"'
    )
    def extrai_dados(data):
        city = '-23.627,-46.655'
        key = 'CRAFXUB8BJLFNR9RVBCD5QYZH'

        URL = os.path.join('https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/',
                           f'{city}/{data}/{ds_add(data,7)}?unitGroup=metric&include=days&key={key}&contentType=csv')

        dados = pd.read_csv(URL)
        file_path = f'/home/felipeflaviobento/Área de Trabalho/ALuraProjects/AirflowDataPipeline/semana={data}/'

        dados.to_csv(file_path + 'dados_brutos.csv')
        dados[['datetime', 'tempmin', 'temp', 'tempmax']].to_csv(file_path + 'temperaturas.csv')
        dados[['datetime', 'description', 'icon']].to_csv(file_path + 'condicoes.csv')
    task2 = PythonOperator(
        task_id = 'extrai_salva_dados',
        python_callable = extrai_dados,
        op_kwargs= {'data':'{{data_interval_end.strftime("%Y-%m-%d")}}'}
    )
    task1 >> task2