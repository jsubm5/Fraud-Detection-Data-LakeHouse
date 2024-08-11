import os, sys, dotenv, unittest

sys.path.insert(0, '/')  # Insert root path to access your modules
dotenv.load_dotenv('/environment.env')

from includes.modules.SparkIcebergNessieMinIO.spark_setup import init_spark_session
from includes.python_scripts.data_operations.helpers.jinja_templates_helper import render_sql_template

class FrauldIncrementLoad(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = init_spark_session('jinja unit test')
        cls.spark.sql('DROP BRANCH IF EXISTS testing IN nessie')
        cls.spark.sql('CREATE BRANCH IF NOT EXISTS testing IN nessie FROM main')
        cls.spark.sql('USE REFERENCE testing IN nessie')

        # Truncate the tables to start with an empty destination
        # cls.spark.sql('TRUNCATE TABLE nessie.bronz_raw_transactions')
        # cls.spark.sql('TRUNCATE TABLE nessie.silver_transactions')

        print('TABLES')
        cls.spark.sql("SHOW TABLES IN nessie").show()
        # Insert initial data into bronz_raw_fraud_tranactions
        cls.spark.sql("""
        INSERT INTO nessie.bronz_raw_transactions
        VALUES (
            'txn001', 
            'sender001', 
            'receiver001', 
            250.00, 
            'USD', 
            TIMESTAMP '2024-08-11 10:15:30', 
            'transfer', 
            'New York', 
            'device001'
        ), 
        (
            'txn002', 
            'sender002', 
            'receiver002', 
            500.50, 
            'EUR', 
            TIMESTAMP '2024-08-11 14:45:00', 
            'payment', 
            'Berlin', 
            'device002'
        );
        """)
        #show tables
        cls.spark.sql('SELECT * FROM nessie.bronz_raw_transactions').show()
        cls.spark.sql('SELECT * FROM nessie.silver_transactions').show()
        cls.spark.sql('SELECT * FROM nessie.months_lookup').show()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def execute_incremental_load(self):
        # transactions are loaded directly once kafka publish it
        batch_args={
            'destination_name'                      :       'nessie.silver_transactions',
            'destination_ingestion_timestamp'       :       'transaction_datetime',
            'source_name'                           :       'nessie.bronz_raw_transactions',
            'source_ingestion_timestamp'            :       'transaction_datetime'
            }

        get_batch_query=render_sql_template(
            template_file='get_incremental_load_batch.sql', 
            **batch_args
            )
        
        self.spark.sql(get_batch_query).createOrReplaceTempView('transactions_temp_view')
        
        load_args={
            'destination_name'                      :       'nessie.silver_transactions',
            'batch_view_or_table_name'              :       'transactions_temp_view',
            'months_lookup_table'                   :       'nessie.months_lookup',
            }

        load_query=render_sql_template(
            template_file='transform_to_silver_transactions.sql',
            **load_args
            )

        self.spark.sql(load_query)
        
        return self.spark.sql('SELECT * FROM nessie.silver_transactions')
        
    def test_incremental_load_destination_is_empty(self):
        result = self.execute_incremental_load()
        res_cnt = result.count()
        src_cnt = self.spark.sql("SELECT * FROM nessie.bronz_raw_transactions").count()
        self.assertEqual(res_cnt, src_cnt, f"Expected {src_cnt} rows but got {res_cnt} rows in the result set.")

    def test_incremental_load_destination_not_empty(self):
        self.spark.sql("""
        INSERT INTO nessie.bronz_raw_transactions
        VALUES (
            'txn001', 
            'sender001', 
            'receiver001', 
            250.00, 
            'USD', 
            TIMESTAMP '2024-08-11 20:15:30', 
            'transfer', 
            'New York', 
            'device001'
        )
        """)
        result = self.execute_incremental_load()
        res_cnt = result.count()
        src_cnt = self.spark.sql("SELECT * FROM nessie.bronz_raw_transactions").count()
        self.assertEqual(res_cnt, src_cnt, f"Expected {src_cnt} rows but got {res_cnt} rows in the result set.")

if __name__ == '__main__':
    unittest.main()
