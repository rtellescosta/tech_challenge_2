import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import logging
from pyspark.sql.functions import col,avg,desc,lag, lead,date_format,to_date,regexp_replace
from pyspark.sql.window import Window
import boto3

# ===========================
# Configuração de Logging
# ===========================

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  # ou DEBUG para mais detalhes

# Adiciona handler só se não existir (evita duplicação de logs no Glue)
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
# ===========================
# Procedure para criar a tabela no glue catalog usando boto
# ===========================

def atualiza_tabela(
    database_name: str,
    table_name: str,
    s3_location: str,
    columns: list,
    partition_keys: list
):
    glue = boto3.client("glue")

    #Garante que o database existe
    try:
        glue.get_database(Name=database_name)
        logger.info(f"database {database_name} encontrado ")
    except :
        glue.create_database(DatabaseInput={"Name": database_name})
        logger.info(f"database {database_name} criado ")
    #Define metadados da tabela
    table_input = {
        "Name": table_name,
        "StorageDescriptor": {
            "Columns": columns,
            "Location": s3_location,
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                "Parameters": {"serialization.format": "1"}
            }
        },
        "PartitionKeys": partition_keys,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {
            "EXTERNAL": "TRUE"
        }
    }

    # Tenta atualizar, senão cria
    try:
        glue.get_table(DatabaseName=database_name, Name=table_name)
        glue.update_table(DatabaseName=database_name, TableInput=table_input)
        logger.info(f"tabela {table_name} atualizada com sucesso ")
       
    except:
        glue.create_table(DatabaseName=database_name, TableInput=table_input)
        logger.info(f"tabela {table_name} criada com sucesso ")    
    

# ===========================
# Inicialização do Glue Job
# ===========================

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# ===========================
# Paths no S3
# ===========================
input_path = "s3://fiapb3/b3_raw/" 
output_path = "s3://fiapb3/b3_refined/"

try:
    
    # ===========================
    #  Leitura dos dados
    # ===========================
    df_input = spark.read.parquet(input_path)
    
    
    # ===========================
    #  Requisitos do trabalho
    #  A: agrupamento numérico, sumarização, contagem ou soma.
    #  B: renomear duas colunas existentes além das de agrupamento.
    #  C: realizar um cálculo com campos de data, exemplo, poder ser du-ração, comparação, diferença entre datas.
    # ===========================
    # ===========================
    #  Alteração de nome de colunas
    # ===========================
    df_acoes =  df_input.withColumnRenamed('dt','data_ingestao')\
                        .withColumnRenamed('Date','data_pregao')
    
    logger.info('Colunas renomeadas')
    
    window_spec = Window.partitionBy('Ticker').orderBy('data_pregao')
    
    df_final = df_acoes.filter(col('Status')=='Close')\
                       .orderBy(col('Ticker'),col('data_pregao'))\
                       .withColumn('prox_valor', lead('Value',1).over(window_spec))\
                       .withColumn(\
                                    "percentual",\
                                    ((col("prox_valor") - col("Value")) / col("Value")) * 100\
                                )\
                       .withColumn('data_pregao', date_format(col('data_pregao'),'yyyy-MM-dd'))\
                       .withColumn('Ticker', regexp_replace(col('Ticker'), r'\.SA$', ''))\
                       .withColumnRenamed('Ticker','ticker')
    
    logger.info('Transformacoes realizadas')
    
    df_final.write.mode('overwrite')\
            .partitionBy('data_pregao','ticker')\
            .parquet(output_path)
    
    logger.info(f'Dados escritos em Parquet no caminho {output_path}, particionados por data_pregao e Ticker')
    
    # ===========================
    # Criaçao/atualiazaco da tabela em um banco ja existente do glue data catalog  
    # ===========================

    logger.info("Iniciando a atualizacao da tabela no Glue Catalog")




    ##COlunas e particao somente com data do pregao 
    
    columns = [
                    {"Name": "Status", "Type": "string"},
                    {"Name": "Value", "Type": "double"},
                    {"Name": "data_ingestao", "Type": "date"},
                    {"Name": "prox_valor", "Type": "double"},
                    {"Name": "percentual", "Type": "double"} 
              ]    
    partition_keys = [
                        {"Name": "data_pregao", "Type": "string"},
                        {"Name": "ticker", "Type": "string"}
                     ]
                     
    atualiza_tabela(
        database_name="bovespa",
        table_name="b3_percentual",
        s3_location="s3://fiapb3/b3_refined/",
        columns=columns,
        partition_keys=partition_keys
    )
    logger.info("Finalizando a atualizacao da tabela no Glue Catalog")
    logger.info("Registrando a tabela criada/atualizada no Glue Catalog")
    # ===========================
    # Registrando a tabela criada/atualizada no Glue Catalog
    # ===========================
    
    athena = boto3.client('athena')
    athena.start_query_execution(
                                    QueryString='MSCK REPAIR TABLE bovespa.b3_percentual;',
                                    QueryExecutionContext={'Database': 'bovespa'},
                                    ResultConfiguration={'OutputLocation': 's3://fiapb3/athena-results/'}
                                )
    
except Exception as e:
    logger.error(f"Ocorreu um erro durante a execução do job: {str(e)}", exc_info=True)
    # Opcional: encerrar o job com erro no Glue
    job.commit()
    sys.exit(1)


job.commit()