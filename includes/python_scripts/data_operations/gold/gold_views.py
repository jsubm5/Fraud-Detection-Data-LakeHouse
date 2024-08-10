"""
 ------------------------------------------------------------
|    This script initializes views in the Gold stage.        |
|    I've used Dremio's API as a workaround since Dremio     |
|    does not support views initialized in Spark SQL.        |
 ------------------------------------------------------------
"""

import sys
sys.path.insert(1, './')

from includes.modules.Dremio.execute_sql import Dremio_sql
from includes.modules.file_utils.dir_handler import DirHandler
from includes.modules.file_utils.file_handler import FileHandler

import dotenv, os
dotenv.load_dotenv('/environment.env')

DREMIO_USERNAME         =os.getenv('DREMIO_USERNAME')
DREMIO_PASSWORD         =os.getenv('DREMIO_PASSWORD')
DREMIO_ENDPOINT         =os.getenv('DREMIO_ENDPOINT')
GOLD_SQL_PATH           =os.getenv('GOLD_SQL_PATH')
VIEW_PATTERN            =os.getenv('VIEW_PATTERN')


dremio_sql = Dremio_sql(username=DREMIO_USERNAME,
                        password=DREMIO_PASSWORD,
                        dremio_endpoint=DREMIO_ENDPOINT)

files_paths = DirHandler(path=GOLD_SQL_PATH)\
                .get_files_with_pattern(pattern=VIEW_PATTERN)

sql_content= FileHandler(path=files_paths)\
        .read_files()

for content in sql_content:
    for query in content.split(';'):
        result = dremio_sql.execute_sql(query)
        print(result)