from pyspark.sql.types import StructType, TimestampType, StructField, StringType, IntegerType, FloatType, DateType

schema = StructType([
    StructField(name='customer_id',
                dataType=StringType(),
                ),
    
    StructField(name='address',
                dataType=StringType()
                ),
        
    StructField(name='current_balance',
                dataType=FloatType(),
                ),
    
    StructField(name='date_of_birth',
                dataType=StringType()
                ),
    
    StructField(name='email',
                dataType=StringType()
                ),
    
    StructField(name='first_name',
                dataType=StringType()
                ),
    
    StructField(name='last_name',
                dataType=StringType()
                ),
    
    StructField(name='phone_number',
                dataType=StringType(),
                ),

    StructField(name='registration_datetime',
               dataType=StringType()
                ),
    
    # for CDC Applications
    StructField(name='ingestion_date', 
               dataType=TimestampType()
                ),
])