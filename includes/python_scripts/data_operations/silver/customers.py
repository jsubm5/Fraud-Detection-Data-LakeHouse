import sys
sys.path.insert(1, '/')
import logging
logger = logging.getLogger(__name__)

from includes.modules.SparkIcebergNessieMinIO.spark_setup import init_spark_session
from includes.modules.Data_cleaner.Strategies import (
    DropDuplicatesStrategy,
    DropMissingValuesStrategy,
)
from includes.modules.Data_cleaner.Interface import CleaningPipeline
from includes.python_scripts.data_operations.helpers.jinja_templates_helper import render_sql_template

# jinja arguments
batch_args={
    'source_name'                           :       'nessie.bronz_raw_customers',
    'source_ingestion_timestamp'            :       'ingested_at',
    'destination_name'                      :       'nessie.silver_customers',
    'destination_ingestion_timestamp'       :       'ingested_at'
    }
get_batch_query=render_sql_template(
    template_file='get_incremental_load_batch.sql', 
    **batch_args
    )

laod_args={
    'destination_name'                      :       'nessie.silver_customers',
    'batch_view_or_table_name'              :       'customers_temp',
    'months_lookup_table'                   :       'nessie.months_lookup'
    }
load_query=render_sql_template(
    template_file='transform_to_silver_customers.sql', 
    **laod_args
    )


try:
    spark = init_spark_session(app_name="clean bronz transactions")

    batch = spark.sql(get_batch_query)

    cleaner = CleaningPipeline()
    cleaner.set_dataframe(df=batch)
    cleaning_strategies = [
        DropDuplicatesStrategy(),
        DropMissingValuesStrategy(),
        # add regex validation later
    ]
    cleaner.add_strategy(cleaning_strategies)
    cleaned_batch = cleaner.run()
    cleaned_batch.createOrReplaceTempView('customers_temp')

    spark.sql(load_query)
    
except Exception as e:
    logger.error(f"An error occurred: {e}")
    raise 
finally:
    spark.stop()