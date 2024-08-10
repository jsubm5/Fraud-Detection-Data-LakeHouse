import pymongo, datetime

def dB_connect(uri:str):
    try:
        client = pymongo.MongoClient(uri)
        return client
    except Exception as e:
        print(e)
        raise ConnectionRefusedError("connection refused")
    finally:
        client.close()
        

def ingest_data(uri:str, db_name:str, collection_name:str)->list:
    client = pymongo.MongoClient(uri)
    db              = client[db_name]
    collection      = db[collection_name]
    ings_timestamp  = datetime.datetime.now()
    
    aggregation_pipeline = [
        {            
            '$addFields': {
                'ingestion_date': ings_timestamp
            }
        },
        {
            '$project': {
            '_id': 0,  
            }
        }
    ]
    data = list(collection.aggregate(aggregation_pipeline))
    client.close()
    return data