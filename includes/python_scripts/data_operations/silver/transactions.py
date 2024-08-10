import sys
sys.path.insert(1, '/')

import logging
logger = logging.getLogger(__name__)

from includes.modules.SparkIcebergNessieMinIO.spark_setup import init_spark_session
from includes.modules.Data_cleaner.Strategies import (
    DropDuplicatesStrategy,
    DropMissingValuesStrategy,
    FilterNegativeValuesStrategy
)
from includes.modules.Data_cleaner.Interface import CleaningPipeline
from includes.python_scripts.data_operations.helpers.jinja_templates_helper import render_sql_template


# preparing jinja arguements
batch_args={
    'destination_name'                      :       'nessie.bronz_raw_transactions',
    'destination_ingestion_timestamp'       :       'ingestion_date',
    'source_name'                           :       'nessie.bronz_raw_transactions',
    'source_ingestion_timestamp'            :       'ingestion_date'
    }

get_batch_query=render_sql_template(
    template_file='get_incremental_load_batch.sql', 
    **batch_args
    )

load_args={
    'destination_name'                      :       'nessie.bronz_raw_transactions',
    'batch_view_or_table_name'              :       'nessie.months_lookup'
    }

load_query=render_sql_template(
    template_file='transform_to_silver_transactions.sql',
    **load_args
    )


spark = init_spark_session(app_name="clean bronz transactions")
sc = spark.sparkContext
sc.setLogLevel("WARN")
try:
    batch = spark.sql(get_batch_query)
    
    batch.show()

    if batch is None:
        raise ValueError("The DataFrame 'batch' is None. Check the data source and distination.")

    cleaner = CleaningPipeline()
    cleaner.set_dataframe(df=batch)

    cleaning_strategies = [
        DropDuplicatesStrategy(),
        DropMissingValuesStrategy(columns=[
            'transaction_id',
            'sender_id',
            'receiver_id',
            'transaction_amount',
            'transaction_currency',
            'transaction_type',
            'transaction_location',
            'device_id'
        ]),
        FilterNegativeValuesStrategy(columns=[
            'transaction_amount'
        ]),
        # Add more as needed, but that is enough as a starting point
    ]

    cleaner.add_strategy(cleaning_strategies)
    cleaned_batch = cleaner.run()
    cleaned_batch.createOrReplaceTempView('transactions_temp_view')

    spark.sql(load_query)

    logger.info("Data cleaning and writing to Silver layer completed successfully.")
except Exception as e:
    logger.error(f"An error occurred: {e}")
finally:
    spark.stop()