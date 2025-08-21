import sys 
import os

DAG_PATH = os.getenv('DAG_PATH', '/opt/airflow/dags/wadua-dag')  
if DAG_PATH not in sys.path:
    sys.path.append(DAG_PATH)

import boto3
import json
import logging as log
from airflow import DAG
from airflow.decorators import task
from airflow.utils.email import send_email
from datetime import datetime, timedelta
from etl import *


def create_task_logger(task_name):
    log_dir = "/tmp/logs_quality"
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"log_{task_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

    logger = logging.getLogger(task_name)
    logger.setLevel(logging.INFO)

    if logger.hasHandlers():
        logger.handlers.clear()

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(console_handler)

    return logger, log_file


def send_success_email(context):
    """Función para enviar un correo electrónico en caso de éxito"""
    subject = "DAG Ejecutado con Éxito: {}".format(context['dag'].dag_id)
    html_content = """
    <h3>DAG Ejecutado con Éxito</h3>
    <p>El DAG <strong>{}</strong> se ejecutó correctamente.</p>
    <p>Fecha de Ejecución: {}</p>
    """.format(context['dag'].dag_id, context['ts'])

    send_email(to="samuelescalanteg@gmail.com", subject=subject, html_content=html_content)

default_args = {
    'owner': 'saalesgu',
    'email': ['samuelescalanteg@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 21),
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

@task
def check_s3_files_task():
    logger, log_file = create_task_logger("check_s3_files")
    try:
        check_s3_files(logger)
    finally:
        upload_log_to_s3(log_file, "wadua-ecommerce-data", f"logs/{os.path.basename(log_file)}")


@task
def invoke_lambda_function_task():
    logger, log_file = create_task_logger("invoke_lambda")
    try:
        return invoke_lambda_function(logger)
    finally:
        upload_log_to_s3(log_file, "wadua-ecommerce-data", f"logs/{os.path.basename(log_file)}")


@task
def consult_db_task():
    logger, log_file = create_task_logger("consult_db")
    try:
        return consult_db(logger)
    finally:
        upload_log_to_s3(log_file, "wadua-ecommerce-data", f"logs/{os.path.basename(log_file)}")

with DAG(
    dag_id="lambda_trigger_with_tasks",
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    on_success_callback=send_success_email

) as dag:
    check_files = check_s3_files_task()
    lambda_response = invoke_lambda_function_task()
    db_response = consult_db_task()

    check_files >> lambda_response >> db_response
