"""
 ------------------------------------------------------------
|    This script initializes views in the Gold stage.        |
|    I've used Dremio's API as a workaround since Dremio     |
|    does not support views initialized in Spark SQL.        |
 ------------------------------------------------------------
"""
import os, dotenv, sys
sys.path.insert(1, './')
dotenv.load_dotenv('./environment.env')

from dremio import DremioAPI

DREMIO_USERNAME     =os.getenv('DREMIO_USERNAME')
DREMIO_PASSWORD     =os.getenv('DREMIO_PASSWORD')
DREMIO_SERVER       =os.getenv('DREMIO_SERVER')

BRONZ_SPACE_NAME    =os.getenv('BRONZ_SPACE_NAME')
SILVER_SPACE_NAME   =os.getenv('SILVER_SPACE_NAME')
GOLD_SPACE_NAME     =os.getenv('GOLD_SPACE_NAME')

BRONZ_SQL_PATH      =os.getenv('BRONZ_SQL_PATH')
SILVER_SQL_PATH     =os.getenv('SILVER_SQL_PATH')
GOLD_SQL_PATH       =os.getenv('GOLD_SQL_PATH')
NESSIE_CATALOG_NAME =os.getenv('NESSIE_CATALOG_NAME')

# dremio instanse
dremio = DremioAPI(
    server      =DREMIO_SERVER,
    username    =DREMIO_USERNAME,
    password    =DREMIO_PASSWORD
)

# create spaces
[dremio.create_space(name=tname) \
    for tname in [BRONZ_SPACE_NAME, SILVER_SPACE_NAME, GOLD_SPACE_NAME]]

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
