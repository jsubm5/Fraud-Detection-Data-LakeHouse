from dotenv import load_dotenv
from pyspark.sql.functions import col, to_timestamp

import os, sys
sys.path.insert(1, '/')
from includes.ingestion.mongoDB.common.schema import fraud
from includes.ingestion.mongoDB.common.ingest import ingest_data
from includes.modules.SparkIcebergNessieMinIO import spark_setup


load_dotenv('/environment.env')

MONGO_URI                   = os.getenv('MONGO_URI')
DATABASE_NAME               = os.getenv('DB_NAME')
FRAUD_COLLECTION_NAME       = os.getenv('FRAUD_COLLECTION_NAME')

data = ingest_data(
        uri                 =MONGO_URI,
        db_name             =DATABASE_NAME,
        collection_name     =FRAUD_COLLECTION_NAME
        )
    
spark = spark_setup.init_spark_session(app_name="fraud_date_ingestion")

df = spark.createDataFrame(data=data,
                           schema=fraud.schema)
df = df.withColumn("labeled_at", to_timestamp(col("labeled_at")))\
        .withColumn("ingestion_date", to_timestamp(col("ingestion_date")))\

df.createTempView('fraud')
spark.sql(
    """
    INSERT INTO nessie.bronz_raw_fraud_tranactions
    (
        transaction_id,
        labeled_at,
        ingested_at 
    )
    SELECT
        transaction_id,
        labeled_at,
        ingestion_date
    FROM
        fraud f
    WHERE
        f.ingestion_date NOT IN (  
            SELECT 
                DISTINCT ingested_at 
            FROM
                nessie.bronz_raw_fraud_tranactions
        )
    AND
        f.transaction_id NOT IN ( 
            SELECT 
                transaction_id
            FROM 
                nessie.bronz_raw_fraud_tranactions
        )
    """
)