import pandas as pd
import sqlalchemy as sa
import datetime as dt

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable


def to_csv(table, path, dwh_con):

    dwh_con = BaseHook.get_connection(dwh_con)
    engine = sa.create_engine(
        f"postgresql://{dwh_con.login}:{dwh_con.password}@{dwh_con.host}:{str(dwh_con.port)}/bank"
    )

    print(f'Читаю таблицу {table} из СУБД')
    df = pd.read_sql(
        f"""
        SELECT *
        FROM dm.{table};
        """,
        engine
    )
    
    print(df)

    print(f'Записываю таблицу {table} в файл {path}')
    df.to_csv(
        path,
        index=False,
    )
    
    print('Запись завершена')


default_args = {
    'owner': 'Швейников Андрей',
    'retries': 4,
    'retry_delay': dt.timedelta(minutes=30),
}
with DAG(
    "to_csv",
    default_args=default_args,
    description="Экспорт в csv",
    start_date=dt.datetime(2024, 1, 3),
    schedule_interval=None,
    tags=['Export', 'csv'],
) as dag:
    
    task = PythonOperator(
        task_id='to_csv',
        python_callable=to_csv,
        op_args=(
            Variable.get('to_csv').split(';')[0].strip(),
            Variable.get('to_csv').split(';')[1].strip(),
            'dwh',
        ),
    )


if __name__ == '__main__':
    to_csv('dm.dm_f101_round_f', '/opt/airflow/Data/output.csv', 'dwh')