import os, sys, dotenv, unittest

sys.path.insert(0, '/')  # Insert root path to access your modules
dotenv.load_dotenv('/environment.env')

from includes.modules.SparkIcebergNessieMinIO.spark_setup import init_spark_session
from includes.python_scripts.data_operations.helpers.jinja_templates_helper import render_sql_template

class FrauldIncrementLoad(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = init_spark_session('jinja unit test')
        cls.spark.sql('CREATE BRANCH IF NOT EXISTS testing IN nessie FROM main')
        cls.spark.sql('USE REFERENCE testing IN nessie')
        print(f'BRANCHES')
        cls.spark.sql('LIST REFERENCES IN nessie').show()
        print('TABLES')
        cls.spark.sql("SHOW TABLES IN nessie").show()

        # Truncate the tables to start with an empty destination
        cls.spark.sql('TRUNCATE TABLE nessie.silver_fraud_transactions')
        cls.spark.sql('TRUNCATE TABLE nessie.bronz_raw_fraud_tranactions')

        # Insert initial data into bronz_raw_fraud_tranactions
        cls.spark.sql("""
            INSERT INTO nessie.bronz_raw_fraud_tranactions (transaction_id, labeled_at, ingested_at)
            VALUES
            ('txn_001', TIMESTAMP '2024-08-01 12:00:00', TIMESTAMP '2024-08-01 12:05:00'),
            ('txn_002', TIMESTAMP '2024-08-02 13:00:00', TIMESTAMP '2024-08-02 13:05:00')
        """)
        #show tables
        cls.spark.sql('SELECT * FROM nessie.bronz_raw_fraud_tranactions').show()
        cls.spark.sql('SELECT * FROM nessie.silver_fraud_transactions').show()
        cls.spark.sql('SELECT * FROM nessie.bronz_raw_transactions').show()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def execute_incremental_load(self):
        batch_args = {
            'source_name': 'nessie.bronz_raw_fraud_tranactions',
            'source_ingestion_timestamp': 'ingested_at',
            'destination_name': 'nessie.silver_fraud_transactions',
            'destination_ingestion_timestamp': 'ingested_at'
        }

        get_batch_query = render_sql_template(
            template_file='get_incremental_load_batch.sql',
            **batch_args
        )
        self.spark.sql(get_batch_query).createOrReplaceTempView('fraud_view')

        load_args = {
            'batch_view_or_table_name': 'fraud_view',
            'bronz_transactions_table_name': 'nessie.bronz_raw_transactions',
            'destination_name': 'nessie.silver_fraud_transactions'
        }

        load_query = render_sql_template(
            template_file='transform_to_silver_fraud.sql',
            **load_args
        )
        self.spark.sql(load_query)
        
        return (self.spark.sql('SELECT * FROM nessie.silver_fraud_transactions'))
    
    def test_incremental_load_destination_is_empty(self):
        result = self.execute_incremental_load()
        res_cnt = result.count()
        src_cnt = self.spark.sql("SELECT COUNT(*) FROM nessie.bronz_raw_fraud_tranactions").collect()[0][0]
        self.assertEqual(res_cnt, src_cnt, f"Expected {src_cnt} rows but got {res_cnt} rows in the result set.")

    def test_incremental_load_destination_not_empty(self):
        self.spark.sql("""
            INSERT INTO nessie.bronz_raw_fraud_tranactions (transaction_id, labeled_at, ingested_at)
            VALUES
            ('txn_003', TIMESTAMP '2024-08-03 12:00:00', TIMESTAMP '2024-08-03 12:05:00'),
            ('txn_004', TIMESTAMP '2024-08-03 13:00:00', TIMESTAMP '2024-08-03 13:05:00')
        """)
        result = self.execute_incremental_load()
        res_cnt = result.count()
        src_cnt = self.spark.sql("SELECT COUNT(*) FROM nessie.bronz_raw_fraud_tranactions").collect()[0][0]
        self.assertEqual(res_cnt, src_cnt, f"Expected {src_cnt} rows but got {res_cnt} rows in the result set.")

if __name__ == '__main__':
    unittest.main()
