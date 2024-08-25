from confluent_kafka import Producer
import json
from bank_app_simulation import BankAppSimulation
import dotenv, os, time


dotenv.load_dotenv('./environment.env')


bas = BankAppSimulation(number_of_branches=3, number_of_customers=3)

conf = {
    'bootstrap.servers' : 'localhost:29092',
    'client.id'         : 'bank-app-simulation-producer'
}

producer = Producer(conf)

def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result.
    Triggered by poll() or flush()."""
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def run_simulation_and_kafka_producer(sending_msgs_num:int=None)->None:
    '''
    set sending_msgs_num to None to send unlimited msgs, it's none by default
    '''
    c = 0
    global bas
    TRANSACTIONS_KAFKA_TOPIC=os.getenv('TRANSACTIONS_KAFKA_TOPIC')

    while True:        
        customer_len_buffer = len(bas.customers_data)
        fraud_transaction_len_buffer = len(bas.fraud_transactions_data)
        new_transactions = bas.simulate()
        
        for transaction in new_transactions:
            producer.produce(
                TRANSACTIONS_KAFKA_TOPIC,
                key=str(transaction['sender_id']),
                value=json.dumps(transaction),
                callback=delivery_report
            )
        
        producer.flush()
        time.sleep(30)
        
        if c == sending_msgs_num:
            break
        if sending_msgs_num:
            c += 1
        if (len(bas.customers_data) != customer_len_buffer) or  (len(bas.fraud_transactions_data) != fraud_transaction_len_buffer):
            load_to_mongodb()
            

def load_to_mongodb():
    global bas
    from pymongo import MongoClient, UpdateOne
    import os, dotenv
    
    dotenv.load_dotenv('./environment.env')
    MONGO_URI                   = os.getenv('MONGO_URI')
    DATABASE_NAME               = os.getenv('DB_NAME')
    CUSTOMERS_COLLECTION_NAME   = os.getenv('CUSTOMERS_COLLECTION_NAME')
    FRAUD_COLLECTION_NAME       = os.getenv('FRAUD_COLLECTION_NAME')

    client                      = MongoClient(MONGO_URI)
    database                    = client[DATABASE_NAME]
    customers_collection        = database[CUSTOMERS_COLLECTION_NAME]
    fraud_collection            = database[FRAUD_COLLECTION_NAME]
    
    # remova all collections
    if database.list_collection_names():
        for collection_name in database.list_collection_names():
            database[collection_name].drop()
        
    fraud_table                 = bas.fraud_transactions_data
    customers_table             = bas.customers_data
    
    fraud_upsert_operations = [
        UpdateOne(
            {"transaction_id"   : record["transaction_id"]}, 
            {"$set"             : record},
            upsert=True
        ) for record in fraud_table
    ]
    customer_upsert_operations = [
        UpdateOne(
            {"customer_id"      : record["customer_id"]}, 
            {"$set"             : record},
            upsert=True
        ) for record in customers_table
    ]
    try:
        if fraud_upsert_operations:
            fraud_collection.bulk_write(fraud_upsert_operations)
        else:
            print("No fraud transactions to upsert.")

        if customer_upsert_operations:
            customers_collection.bulk_write(customer_upsert_operations)
        else:
            print("No customer records to upsert.")
    except Exception as e:
        print(e)
    finally:        
        client.close()
    client.close()
if __name__ == '__main__':
    run_simulation_and_kafka_producer(10)
    # load_to_mongodb()
    # print(f"\n\ncustomers num: {len(bas.customers_data)}")