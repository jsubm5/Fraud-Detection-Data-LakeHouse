from dotenv import load_dotenv
from pyspark.sql.functions import col, to_timestamp, to_date

import os, sys
sys.path.insert(1, '/')
from includes.ingestion.mongoDB.common.schema import customers
from includes.ingestion.mongoDB.common.ingest import ingest_data
from includes.modules.SparkIcebergNessieMinIO import spark_setup

load_dotenv('/environment.env')

MONGO_URI                   = os.getenv('MONGO_URI')
DATABASE_NAME               = os.getenv('DB_NAME')
CUSTOMERS_COLLECTION_NAME   = os.getenv('CUSTOMERS_COLLECTION_NAME')

data = ingest_data(
        uri               =MONGO_URI,
        db_name           =DATABASE_NAME,
        collection_name   =CUSTOMERS_COLLECTION_NAME
        )

spark = spark_setup.init_spark_session(app_name="customer_date_ingestion")

df = spark.createDataFrame(data=data,
                           schema=customers.schema)
df = df.withColumn("date_of_birth", 
                   to_date(col("date_of_birth")))\
        .withColumn("registration_datetime", 
                   to_timestamp(col("registration_datetime")))

df.createTempView('Customers')

# very simple ingestion technique, this query needs optmizations
spark.sql(
    """
    REPLACE TABLE nessie.bronz_raw_customers
    USING iceberg
    AS
    SELECT
        first_name AS first_name,
        last_name AS last_name,
        phone_number AS phone_number,
        email AS email,
        address AS c_address,
        date_of_birth AS birth_date,
        customer_id AS customer_id,
        registration_datetime AS registration_datetime,
        ingested_at AS ingested_at
    FROM
        Customers
    """
)