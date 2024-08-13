import sys
sys.path.insert(1, '/')
from includes.modules.SparkIcebergNessieMinIO.spark_setup import init_spark_session



spark=init_spark_session('explorer')
spark.sparkContext.setLogLevel("ERROR")

tables= spark.sql("SHOW TABLES IN nessie")
print("\n\nT A B L E S   I N   N E S S I E:\n")
tables.show()
table_names = [row.tableName for row in tables.collect()]

for table_name in table_names:
    table=spark.sql(f"SELECT * FROM nessie.{table_name}")
    print(f"\n\nTable: {table_name}")
    table.show()