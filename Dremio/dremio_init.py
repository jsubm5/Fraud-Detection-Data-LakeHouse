"""
 ------------------------------------------------------------
|    This script initializes views in the Gold stage.        |
|    I've used Dremio's API as a workaround since Dremio     |
|    does not support views initialized in Spark SQL.        |
 ------------------------------------------------------------
"""
import os, dotenv, sys
# sys.path.insert(1, './')
dotenv.load_dotenv('./environment.env')

from dremio import DremioAPI

DREMIO_USERNAME     =os.getenv('DREMIO_USERNAME')
DREMIO_PASSWORD     =os.getenv('DREMIO_PASSWORD')
DREMIO_ENDPOINT     =os.getenv('DREMIO_ENDPOINT')

BRONZ_SPACE_NAME    =os.getenv('BRONZ_SPACE_NAME')
SILVER_SPACE_NAME   =os.getenv('SILVER_SPACE_NAME')
GOLD_SPACE_NAME     =os.getenv('GOLD_SPACE_NAME')

BRONZ_SQL_PATH      =os.getenv('BRONZ_SQL_PATH')
SILVER_SQL_PATH     =os.getenv('SILVER_SQL_PATH')
GOLD_SQL_PATH       =os.getenv('GOLD_SQL_PATH')
NESSIE_CATALOG_NAME =os.getenv('NESSIE_CATALOG_NAME')

# dremio instanse
dremio = DremioAPI(
    server      =DREMIO_ENDPOINT,
    username    =DREMIO_USERNAME,
    password    =DREMIO_PASSWORD
)

# create spaces
try:
    [dremio.create_space(name=tname) \
        for tname in [BRONZ_SPACE_NAME, SILVER_SPACE_NAME, GOLD_SPACE_NAME]]
except Exception as e:
    print(f"An error occurred: {e}")
    
# create views
bronz_tables=[
    f'{NESSIE_CATALOG_NAME}.bronz_raw_customers',
    f'{NESSIE_CATALOG_NAME}.bronz_raw_fraud_tranactions',
    f'{NESSIE_CATALOG_NAME}.bronz_raw_transactions'
]

silver_tables=[
    f'{NESSIE_CATALOG_NAME}.silver_customers',
    f'{NESSIE_CATALOG_NAME}.silver_fraud_transactions',
    f'{NESSIE_CATALOG_NAME}.silver_transactions'
]

table_names = [*bronz_tables, *silver_tables]
for table_name in table_names:
    view_name=f"view_{table_name.split('.')[-1]}"
    
    target_path = (BRONZ_SPACE_NAME if 'bronz' in view_name.lower() else
               SILVER_SPACE_NAME if 'silver' in view_name.lower() else
               (lambda: (_ for _ in ()).throw(KeyError("Target space can't be recognized for table {table_name}".format(table_name=table_name))))())
    try:
        dremio.create_view(
            name=view_name,
            path=target_path,
            query=f"""
                SELECT 
                    * 
                FROM 
                    {table_name} 
                AT 
                    BRANCH \"main\";
            """
        )
    except Exception as e:
        print(f"An error occurred: {e}")

try:
    dremio.create_view(
        name='view_fraud_summary',
        path=GOLD_SPACE_NAME,
        query='''
        SELECT
            t.sender_id,
            COUNT(f.transaction_id) AS fraud_transactions_count,
            SUM(t.transaction_amount) AS total_fraud_amount,
            AVG(t.transaction_amount) AS avg_fraud_amount
        FROM 
            nessie.silver_fraud_transactions AT BRANCH "main" f
        JOIN 
            nessie.silver_transactions AT BRANCH "main" t 
        ON 
            f.transaction_id = t.transaction_id
        GROUP BY 
            t.sender_id;
        '''
    )
except Exception as e:
    print(f"An error occurred: {e}")
    
try:
    dremio.create_view(
        name='view_transaction_summary',
        path=GOLD_SPACE_NAME,
        query='''
        SELECT
            sender_id,
            COUNT(transaction_id) AS total_transactions,
            SUM(transaction_amount) AS total_amount,
            AVG(transaction_amount) AS avg_transaction_amount,
            MIN(transaction_datetime) AS first_transaction_date,
            MAX(transaction_datetime) AS last_transaction_date
        FROM nessie.silver_transactions AT BRANCH "main"
        GROUP BY sender_id
        '''
    )
except Exception as e:
    print(f"An error occurred: {e}")