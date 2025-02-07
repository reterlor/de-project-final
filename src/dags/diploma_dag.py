from datetime import datetime, timedelta
from decimal import Decimal

from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.vertica.hooks.vertica import VerticaHook

def extract_from_postgres(execution_date):
    """
    Извлекает данные о курсах валют из PostgreSQL за предыдущий день.

    Аргументы:
        execution_date: Дата выполнения.

    Возвращает:
        flattened_values: Список значений, содержащих дату обновления, код валюты,
                          код валюты для обмена и коэффициент обмена, приведённый к строке.
    """
    execution_date = datetime.strptime(execution_date, '%Y-%m-%d').date()
    data_date = execution_date - timedelta(days=1)
    pg_hook = PostgresHook(postgres_conn_id='postgre-diploma')
    query = f"""
                SELECT date_update, currency_code, currency_code_with, currency_with_div 
                FROM public.currencies 
                WHERE date_update = '{data_date}';
            """
    records = pg_hook.get_records(query)

    flattened_values = [
        item for r in records for item in (str(r[0]), r[1], r[2], str(Decimal(r[3])))
    ]

    return flattened_values

def load_into_vertica(ti):
    """
    Загружает данные о курсах валют в Vertica.

    Аргументы:
        ti: Объект задачи Airflow для извлечения данных.
    """
    vertica_hook = VerticaHook(vertica_conn_id='vertica-diploma')
    flattened_values = ti.xcom_pull(task_ids='extract_from_postgres_currencies')

    if flattened_values:
        num_records = len(flattened_values) // 4  

        insert_query = """
                        INSERT INTO STV2024101049__STAGING.currencies 
                        (date_update, currency_code, currency_code_with, currency_code_div) 
                        VALUES {}
                      """.format(", ".join(["(%s, %s, %s, %s)"] * num_records))

        vertica_hook.run(insert_query, parameters=flattened_values)

def extract_from_postgres_transactions(execution_date):
    """
    Извлекает данные о транзакциях из PostgreSQL за предыдущий день.

    Аргументы:
        execution_date: Дата выполнения.

    Возвращает:
        flattened_values: Список значений, содержащих идентификатор операции, номера счетов,
                          код валюты, страну, статус, тип транзакции, сумму и дату транзакции.
    """
    execution_date = datetime.strptime(execution_date, '%Y-%m-%d').date()
    data_date = execution_date - timedelta(days=1)
    pg_hook = PostgresHook(postgres_conn_id='postgre-diploma')
    query = f"""
                SELECT operation_id, account_number_from, account_number_to, currency_code, country, status, transaction_type, amount, transaction_dt 
                FROM public.transactions 
                WHERE transaction_dt >= '{data_date}' AND transaction_dt < TIMESTAMP '{execution_date}';
            """
    records = pg_hook.get_records(query)

    formatted_records = [
        (r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], str(r[8])) for r in records
    ]
    flattened_values = [item for sublist in formatted_records for item in sublist]

    return flattened_values

def load_into_vertica_transactions(ti):
    """
    Загружает данные о транзакциях в Vertica.

    Аргументы:
        ti: Объект задачи Airflow для извлечения данных.
    """
    vertica_hook = VerticaHook(vertica_conn_id='vertica-diploma')
    flattened_values = ti.xcom_pull(task_ids='extract_from_postgres_transactions')

    if flattened_values:
        num_records = len(flattened_values) // 9

        insert_query = """
                        INSERT INTO STV2024101049__STAGING.transactions 
                        (operation_id, account_number_from, account_number_to, currency_code, country, status, transaction_type, amount, transaction_dt) 
                        VALUES {}
                      """.format(", ".join(["(%s, %s, %s, %s, %s, %s, %s, %s, %s)"] * num_records))

        vertica_hook.run(insert_query, parameters=flattened_values)

def load_into_vertica_global_metrics(execution_date):
    """
    Заполняет витрину в Vertica глобальными метриками за предыдущий день, выполняя SQL-запрос из файла.

    Аргументы:
        execution_date: Дата выполнения.

    """
    vertica_hook = VerticaHook(vertica_conn_id='vertica-diploma')
    execution_date = datetime.strptime(execution_date, '%Y-%m-%d').date()
    data_date = execution_date - timedelta(days=1)
    with open('/lessons/dags/SQL_global_metrics.sql', 'r') as file:
        query = file.read()
    query = query.format(data_date=data_date, execution_date=execution_date)
    vertica_hook.run(query)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 10, 10),
    'retries': 1,
}

dag = DAG(
    'diploma_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=True,
)

extract_currencies = PythonOperator(
    task_id='extract_from_postgres_currencies',
    python_callable=extract_from_postgres,
    op_kwargs={'execution_date': '{{ ds }}'},
    provide_context=True,
    dag=dag,
)

load_currencies = PythonOperator(
    task_id='load_into_vertica_currencies',
    python_callable=load_into_vertica,
    provide_context=True,
    dag=dag,
)

extract_transactions = PythonOperator(
    task_id='extract_from_postgres_transactions',
    python_callable=extract_from_postgres_transactions,
    op_kwargs={'execution_date': '{{ ds }}'},
    provide_context=True,
    dag=dag,
)

load_transactions = PythonOperator(
    task_id='load_into_vertica_transactions',
    python_callable=load_into_vertica_transactions,
    provide_context=True,
    dag=dag,
)

load_global_metrics = PythonOperator(
    task_id='load_into_vertica_global_metrics',
    python_callable=load_into_vertica_global_metrics,
    op_kwargs={'execution_date': '{{ ds }}'},
    provide_context=True,
    dag=dag,
)


extract_currencies >> load_currencies
extract_transactions >> load_transactions

load_currencies >> load_global_metrics
load_transactions >> load_global_metrics