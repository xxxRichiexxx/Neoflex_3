import pandas as pd
import sqlalchemy as sa
import datetime as dt

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable


def from_csv(table, path, dwh_con):

    dwh_con = BaseHook.get_connection(dwh_con)
    engine = sa.create_engine(
        f"postgresql://{dwh_con.login}:{dwh_con.password}@{dwh_con.host}:{str(dwh_con.port)}/bank"
    )

    print(f'Читаем из файла {path}')
    df = pd.read_csv(
        path,
    )
    
    print(df)

    print(f'Записываем в таблицу {table}')
    df.to_sql(
        table,
        engine,
        'dm',
        'append',
        False,
    )
    print('Запись завершена')
    

default_args = {
    'owner': 'Швейников Андрей',
    'retries': 4,
    'retry_delay': dt.timedelta(minutes=30),
}
with DAG(
    "from_csv",
    default_args=default_args,
    description="Импорт из csv",
    start_date=dt.datetime(2024, 1, 3),
    schedule_interval=None,
    tags=['Import', 'csv'],
) as dag:
    
    task = PythonOperator(
        task_id='from_csv',
        python_callable=from_csv,
        op_args=(
            Variable.get('from_csv').split(';')[0].strip(),
            Variable.get('from_csv').split(';')[1].strip(),
            'dwh',
        ),
    )


if __name__ == '__main__':
    from_csv('dm_account_turnover_f_test', '/opt/airflow/Data/output.csv', 'dwh')