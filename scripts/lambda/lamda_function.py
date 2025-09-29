import json
import boto3
import re
import urllib.parse

glue = boto3.client("glue")

def lambda_handler(event, context):
    """
    Lambda disparada por evento de criação de objeto no S3.
    Inicia o Glue Job b3_raw_stage passando a partição dt como argumento.
    """
    try:
        # Pega o registro do evento S3
        record = event["Records"][0]
        s3_object_key = record["s3"]["object"]["key"]  # ex: b3_raw/dt=2025-09-17/b3_stock_info.parquet

        # Decodifica a key (resolve o %3D)
        decoded_key = urllib.parse.unquote_plus(s3_object_key)
        print(f"Decoded key: {decoded_key}")

        # Extrai a data da partição com regex
        
        match = re.search(r"dt=(\d{4}-\d{2}-\d{2})", decoded_key)
        if not match:
            raise ValueError(f"Não foi possível extrair a partição do caminho: {s3_object_key}")

        partition_dt = match.group(1)

        # Nome do job Glue
        glue_job_name = "b3_raw_stage"

        # Dispara o Glue Job com argumento de data
        response = glue.start_job_run(
            JobName=glue_job_name,
            Arguments={
                "--dt": partition_dt
            }
        )

        print(f"Glue job {glue_job_name} iniciado com sucesso para partição {partition_dt}.")
        return {
            "statusCode": 200,
            "body": json.dumps("Job iniciado com sucesso"),
            "jobRunId": response["JobRunId"]
        }

    except Exception as e:
        print(f"Erro ao processar evento: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps(str(e))
        }
