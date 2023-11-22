import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook


def _create_normalized_in_ch():
    query = f"""
        CREATE TABLE IF NOT EXISTS cannabis ON CLUSTER {Variable.get("ch_company_cluster")}
        (
            id Int64 NULL,
            uid`UUID NOT NULL,
            strain String NULL,
            cannabinoid_abbreviation String NULL,
            cannabinoid String NULL,
            terpene String NULL,
            medical_use String NULL,
            health_benefit String NULL,
            category String NULL,
            type String NULL,
            buzzword String NULL,
            brand String NULL
        )
        Engine=MergeTree()
        ORDER BY id;
        """
    ch_hook = ClickHouseHook(clickhouse_conn_id="clickhouse", query_id='{{ ti.dag_id }}-{{ ti.task_id }}-{{ ti.run_id }}-{{ ti.try_number }}',)
    ch_hook.execute(sql=query)

def _dump_results(response) -> str:
    import json
    today = datetime.datetime.now().strftime('%d/%m/%Y/%H:%M:%S')
    with open(f"tmp/{today}", "w") as j:
        json.dump(response.text, j)
    return today

def _insert_to_ch(ti):
    import json
    ch_hook = ClickHouseHook(clickhouse_conn_id="clickhouse", query_id='{{ ti.dag_id }}-{{ ti.task_id }}-{{ ti.run_id }}-{{ ti.try_number }}',)
    with open(ti.xcom_pull(task_ids="extract_data"), "r") as j:
        data = json.load(j)
    ch_hook.execute("INSERT INTO cannabis FORMAT JSON", data)

with DAG(
    dag_id="aero_cannabis_ch",
    description="Даг с ClickHouse плагином",
    start_date=datetime.datetime.now(),
    schedule="0 0 0/12 ? *",
    catchup=False
) as dag:
    
    dag.doc_md = """
    Приветствую!
    ### Немного про реализацию
    Не совсем понял из задания нужно ли чтобы данные были нормализованы или нет.
    Оставил нормализованный вариант (_create_normalized_in_ch), так как тип JSON
    является экспериментальным в кликхаус. Наверное, если бы писал коннектор к постгрес, положил бы в этот тип,
    раз мы придерживаемся ELT, а не ETL. 
    ### Про кликхаус и опять про реализацию :)
    В целом, выбрал кликхаус, так как ранее не писал Даги с ним, плюс, он хорошо подходит для чтения,
    которое потом будет на Transform этапе.
    Функционал конкретного дага можно расширить, например, проверяя в начале есть ли файл с сегодняшней датой,
    также можно удалять временные файлы за собой. Я предпочитаю оставлять, чтобы потом легко выяснить в чем было дело
    при неудачном запуске.
    На этом все. Надеюсь реализация понравилась :3
    """

    is_api_available = HttpSensor(
        task_id="is_api_available",
        endpoint="/cannabis/random_cannabis?size=1",
        http_conn_id="cannabis_api",
    )
    
    create_table = PythonOperator(
        task_id='create_table',
        python_callable=_create_normalized_in_ch,
    )

    extract_data = SimpleHttpOperator(
        task_id="extract_data", endpoint=f"/cannabis/random_cannabis?size={Variable.get('BATCH_SIZE')}",
        http_conn_id="cannabis_api", method="GET", response_filter=_dump_results)

    insert_data = PythonOperator(
        task_id='insert_data',
        python_callable=_insert_to_ch,
    )

    is_api_available >> create_table >> extract_data >> insert_data