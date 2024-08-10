import sys
sys.path.insert(1, '/')

from includes.modules.file_utils.dir_handler import DirHandler
from includes.modules.file_utils.file_handler import FileHandler
from includes.modules.SparkIcebergNessieMinIO.spark_setup import init_spark_session

import os, dotenv
dotenv.load_dotenv('/environment.env')

LOOKUP_SQL_PATH     =os.getenv('LOOKUP_SQL_PATH')
BRONZ_SQL_PATH      =os.getenv('BRONZ_SQL_PATH')
SILVER_SQL_PATH     =os.getenv('SILVER_SQL_PATH')
CREATE_PATTERN      =os.getenv('CREATE_PATTERN')

init_sql_dir_paths = [BRONZ_SQL_PATH, SILVER_SQL_PATH, LOOKUP_SQL_PATH]

sql_paths = DirHandler(path=init_sql_dir_paths)\
    .get_files_with_pattern(CREATE_PATTERN)
    
sql_content = FileHandler(path=sql_paths)\
    .read_files()
    
spark = init_spark_session(app_name="DLH tables init")

for content in sql_content:
    print(f'\n\n{content}\n\n')
    for query in content.split(';'):
        if query.strip():
            spark.sql(query)