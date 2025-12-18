from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.decorators import task_group
from datetime import datetime, timedelta
import logging
import csv
import os


def on_success_callback(context):
    dag_id = context["dag"].dag_id
    execution_date = context["execution_date"]
    print(f"DAG {dag_id} успешно выполнен за {execution_date}")


def on_failure_callback(context):
    dag_id = context["dag"].dag_id
    error = context.get("exception")
    print(f"DAG {dag_id} завершён с ошибкой:\n{error}")


def check_top_clients_not_empty():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    sql = open("/opt/airflow/dags/sql/top_clients.sql").read()
    sql = sql.rstrip(";")
    count = hook.get_records(f"SELECT COUNT(*) as total_rows FROM ({sql}) t;")[0][0]

    if count == 0:
        raise Exception("Запрос top_clients вернул 0 строк")
    else:
        print(f"Всего строк: {count}")

def check_wealth_top_clients_not_empty():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    sql = open("/opt/airflow/dags/sql/wealth_segment_top_clients.sql").read()
    sql = sql.rstrip(";")
    count = hook.get_records(f"SELECT COUNT(*) as total_rows FROM ({sql}) t;")[0][0]

    if count == 0:
        raise Exception("Запрос top_clients вернул 0 строк")
    else:
        print(f"Всего строк: {count}")

def export_top_clients_to_csv():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    sql = open("/opt/airflow/dags/sql/top_clients.sql").read()
    result = hook.get_records(sql)
    output_file = "/opt/airflow/data/top_clients.csv"
    with open(output_file, mode="w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["customer_id", "first_name", "last_name", "sum_sales", "dr"])
        for row in result:
            writer.writerow(row)


def export_wealth_top_clients_to_csv():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    sql = open("/opt/airflow/dags/sql/wealth_segment_top_clients.sql").read()
    result = hook.get_records(sql)
    output_file = "/opt/airflow/data/wealth_segment_top_clients.csv"
    with open(output_file, mode="w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["first_name", "last_name", "wealth_segment", "sum_sales"])
        for row in result:
            writer.writerow(row)
    

# Определяем аргументы по умолчанию для DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_success_callback': on_success_callback,
    'on_failure_callback': on_failure_callback
}


with DAG(
    dag_id='Kruchkova_Yulia_N_hw_06',
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args
) as dag:
    @task_group
    def create_tables_group():
        # 1. Задача для создания таблицы customer в PostgreSQL
        create_customer_table_task = PostgresOperator(
            task_id='create_customer_table_if_not_exists',
            postgres_conn_id='postgres_default',      # подключение через AirFlow UI
            
            sql="""
                drop table if exists customer;
                create table if not exists customer (
                     customer_id    int
                    ,first_name text
                    ,last_name     text
                    ,gender     text
                    ,dob     text
                    ,job_title     text
                    ,job_industry_category     text
                    ,wealth_segment     text
                    ,deceased_indicator     text
                    ,owns_car     text
                    ,address     text
                    ,postcode     text
                    ,state     text
                    ,country     text
                    ,property_valuation int
                );
            """,
        )
    
        # 2. Задача для создания таблицы product в PostgreSQL
        create_product_table_task = PostgresOperator(
            task_id='create_product_table_if_not_exists',
            postgres_conn_id='postgres_default',      # подключение через AirFlow UI
            
            sql="""
                drop table if exists product;
                create table if not exists product (
                     product_id    int
                    ,brand text
                    ,product_line text
                    ,product_class     text
                    ,product_size     text
                    ,list_price     numeric(10, 2)
                    ,standard_cost     numeric(10, 2)
                );
            """,
        )
    
        # 3. Задача для создания таблицы orders в PostgreSQL
        create_orders_table_task = PostgresOperator(
            task_id='create_orders_table_if_not_exists',
            postgres_conn_id='postgres_default',      # подключение через AirFlow UI
            
            sql="""
                drop table if exists orders;
                create table if not exists orders (
                     order_id    int
                    ,customer_id    int
                    ,order_date text
                    ,online_order     text
                    ,order_status     text
                );
            """,
        )
    
        # 4. Задача для создания таблицы order_items в PostgreSQL
        create_order_items_table_task = PostgresOperator(
            task_id='create_order_items_table_if_not_exists',
            postgres_conn_id='postgres_default',      # подключение через AirFlow UI
            
            sql="""
                drop table if exists order_items;
                create table if not exists order_items (
                     order_item_id    int
                    ,order_id    int
                    ,product_id    int
                    ,quantity    float
                    ,item_list_price_at_sale     numeric(10, 2)
                    ,item_standard_cost_at_sale     numeric(10, 2)
                );
            """,
        )
    
    
    @task_group
    def load_data_group():
        load_customer_data = PostgresOperator(
            task_id='load_customer_data',
            postgres_conn_id='postgres_default',
            sql="""
                COPY customer FROM '/tmp/data_files/customer.csv'
                WITH (FORMAT CSV, HEADER true, DELIMITER ';');
            """
        )
        
        load_product_data = PostgresOperator(
            task_id='load_product_data',
            postgres_conn_id='postgres_default',
            sql="""            
                COPY product FROM '/tmp/data_files/product.csv'
                WITH (FORMAT CSV, HEADER true, DELIMITER ',');
            """
        )
        
        load_orders_data = PostgresOperator(
            task_id='load_orders_data',
            postgres_conn_id='postgres_default',
            sql="""            
                COPY orders FROM '/tmp/data_files/orders.csv'
                WITH (FORMAT CSV, HEADER true, DELIMITER ',');
            """
        )
        
        load_order_items_data = PostgresOperator(
            task_id='load_order_items_data',
            postgres_conn_id='postgres_default',
            sql="""           
                COPY order_items FROM '/tmp/data_files/order_items.csv'
                WITH (FORMAT CSV, HEADER true, DELIMITER ',');
            """
        )
    
    
    @task_group
    def check_clients_group():
        check_top_clients = PythonOperator(
            task_id="check_top_clients",
            python_callable=check_top_clients_not_empty,
        )
        
        check_wealth_top_clients = PythonOperator(
            task_id="check_wealth_top_clients",
            python_callable=check_wealth_top_clients_not_empty,
        )
    
    
    @task_group
    def export_top_clients_group():
        top_clients_to_csv = PythonOperator(
            task_id="export_top_clients",
            python_callable=export_top_clients_to_csv
        )
        
        wealth_top_clients_to_csv = PythonOperator(
            task_id="export_wealth_segment_top_clients",
            python_callable=export_wealth_top_clients_to_csv
        )
    
    
    # Определение зависимостей
    create_tables_group() >> load_data_group() >> check_clients_group() >> export_top_clients_group()
