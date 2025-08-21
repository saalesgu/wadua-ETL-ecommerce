import boto3
import json
import logging
import duckdb
import os
from datetime import datetime
import boto3

log_dir = "/tmp/logs_quality"
os.makedirs(log_dir, exist_ok=True)

# Nombre del archivo con fecha y hora
log_file = os.path.join(log_dir, f"log_quality_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

# Configuración del logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()  # opcional, para ver logs en consola
    ]
)

logger = logging.getLogger()


def upload_log_to_s3(file_path, bucket_name, s3_key):
    s3_client = boto3.client('s3')
    try:
        s3_client.upload_file(file_path, bucket_name, s3_key)
        logger.info(f"Log subido correctamente a s3://{bucket_name}/{s3_key}")
    except Exception as e:
        logger.error(f"No se pudo subir el log a S3: {e}")


def check_s3_files(logger):
    s3 = boto3.client("s3")
    bucket = "wadua-ecommerce-data"
    prefix = "raw-data/"

    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    files = [obj["Key"] for obj in response.get("Contents", [])]

    if not files:
        logger.warning("⚠️ No se encontraron archivos en S3")
        raise ValueError("No se encontraron archivos en S3")
    else:
        logger.info(f"Archivos detectados en S3: {files}")


def invoke_lambda_function(logger):
    logger.info("Invocando Lambda...")
    try:
        lambda_client = boto3.client("lambda")
        response = lambda_client.invoke(
            FunctionName="wadua-lambda-function",
            InvocationType="RequestResponse"
        )
        response_payload = json.loads(response["Payload"].read().decode("utf-8"))

        if response_payload.get("status") == 200:
            logger.info(f"Respuesta Lambda exitosa: {response_payload}")
        else:
            logger.warning(f"Respuesta Lambda no exitosa: {response_payload}")

        return response_payload
    except Exception as e:
        logger.error(f"Error invocando Lambda: {e}")
        raise


def consult_db(logger):
    logger.info("Consultando la base de datos...")
    conn = duckdb.connect("md:wadua-db?motherduck_token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6InNhbXVlbGVzY2FsYW50ZWdAZ21haWwuY29tIiwic2Vzc2lvbiI6InNhbXVlbGVzY2FsYW50ZWcuZ21haWwuY29tIiwicGF0IjoiLUw1M1ZFSG9seXdiZ1pTTW9FN2l1bnV4ZDFxMXJ1Z0Q3cl9FekdPWFNNbyIsInVzZXJJZCI6ImVmMzgwMjllLTE2OWUtNDFlZS04NDcwLTJiZDc5NjNhOTk3OSIsImlzcyI6Im1kX3BhdCIsInJlYWRPbmx5IjpmYWxzZSwidG9rZW5UeXBlIjoicmVhZF93cml0ZSIsImlhdCI6MTc1NDkyMTA2N30.SYuDQVbZVOLo4riP8X6iTMoGkEeYlZ-nr-k2mcZg1is")
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) AS total_filas FROM customer")
    resultados = cursor.fetchall()
    logger.info(f"Total de filas en customer: {resultados[0][0]}")
    conn.close()
    return {"data": resultados}

