from pyspark.sql.types import StructType, TimestampType, StructField, StringType, IntegerType, FloatType, DateType

schema = StructType([
    StructField(name='transaction_id',
                dataType=StringType(),
                ),
    
    StructField(name='labeled_at',
                dataType=StringType()
                ),
    
    # for CDC Applications
    StructField(name='ingestion_date', 
               dataType=TimestampType()
                ),
])