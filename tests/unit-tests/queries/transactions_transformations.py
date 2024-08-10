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

        # Truncate the tables to start with an empty destination
        cls.spark.sql('TRUNCATE TABLE nessie.bronz_raw_customers')
        cls.spark.sql('TRUNCATE TABLE nessie.silver_customers')

        print('TABLES')
        cls.spark.sql("SHOW TABLES IN nessie").show()
        # Insert initial data into bronz_raw_fraud_tranactions
        cls.spark.sql("""
        INSERT INTO nessie.bronz_raw_customers (
            first_name, 
            last_name, 
            phone_number, 
            email, 
            c_address, 
            birth_date, 
            customer_id, 
            registration_datetime, 
            ingested_at
        ) VALUES 
            ('John', 'Doe', '555-1234', 'john.doe@example.com', '456 Maple St, Springfield, IL', DATE '1990-01-15', 'cust_001', TIMESTAMP '2024-08-10 09:00:00',   TIMESTAMP '2024-08-10 09:05:00'),
            ('Jane', 'Smith', '555-5678', 'jane.smith@example.com', '789 Oak St, Springfield, IL', DATE '1992-07-22', 'cust_002', TIMESTAMP '2024-08-11 10:00:00', TIMESTAMP '2024-08-10 10:05:00')
        """)
        #show tables
        cls.spark.sql('SELECT * FROM nessie.bronz_raw_customers').show()
        cls.spark.sql('SELECT * FROM nessie.silver_customers').show()
        cls.spark.sql('SELECT * FROM nessie.months_lookup').show()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def execute_incremental_load(self):
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
        
        self.spark.sql(get_batch_query).createOrReplaceTempView('customers_temp')
        
        laod_args={
            'destination_name'                      :       'nessie.silver_customers',
            'batch_view_or_table_name'              :       'customers_temp',
            'months_lookup_table'                   :       'nessie.months_lookup'
            }
        load_query=render_sql_template(
            template_file='transform_to_silver_customers.sql', 
            **laod_args
            )

        self.spark.sql(load_query)
        
        return self.spark.sql('SELECT * FROM nessie.silver_customers')
        
    def test_incremental_load_destination_is_empty(self):
        result = self.execute_incremental_load()
        res_cnt = result.count()
        src_cnt = self.spark.sql("SELECT * FROM nessie.bronz_raw_customers").count()
        self.assertEqual(res_cnt, src_cnt, f"Expected {src_cnt} rows but got {res_cnt} rows in the result set.")

    def test_incremental_load_destination_not_empty(self):
        self.spark.sql("""
        INSERT INTO nessie.bronz_raw_customers (
            first_name, 
            last_name, 
            phone_number, 
            email, 
            c_address, 
            birth_date, 
            customer_id, 
            registration_datetime, 
            ingested_at
        ) VALUES 
            ('John', 'Doe', '555-1234', 'john.doe@example.com', '456 Maple St, Springfield, IL', DATE '1990-01-15', 'cust_001', TIMESTAMP '2024-08-10 09:00:00',   TIMESTAMP '2024-08-11 09:05:00'),
            ('Jane', 'Smith', '555-5678', 'jane.smith@example.com', '789 Oak St, Springfield, IL', DATE '1992-07-22', 'cust_002', TIMESTAMP '2024-08-11 10:00:00', TIMESTAMP '2024-08-11 10:05:00')
        """)
        result = self.execute_incremental_load()
        res_cnt = result.count()
        src_cnt = self.spark.sql("SELECT * FROM nessie.bronz_raw_customers").count()
        self.assertEqual(res_cnt, src_cnt, f"Expected {src_cnt} rows but got {res_cnt} rows in the result set.")

if __name__ == '__main__':
    unittest.main()
