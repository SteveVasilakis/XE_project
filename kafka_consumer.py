from kafka import KafkaConsumer, TopicPartition
from kafka.errors import NoBrokersAvailable
import dateutil.parser
import pymysql
import os, json, ast, datetime, re



host = 'svdataengineer.cg2t1fioak49.eu-west-3.rds.amazonaws.com'
user = 'root'
pwd = 'UfPxTfNcukx54YT9HRApkvsmLLzFTa39'
db = 'svdataengineer'

def connect_to_mysql():
    try:
        conn = pymysql.connect(host=host,user=user,password = pwd, database=db)
        return conn
    except pymysql.OperationalError:
        print("Access Denied")

def add_new_event(id, customer_id, created_at, text, ad_type, price, currency, payment_type, payment_cost):    
    conn = connect_to_mysql()
    cursor = conn.cursor()    
    cursor.execute("INSERT INTO Classifieds (id, customer_id, created_at, text, ad_type, price, currency, payment_type, payment_cost)\
                     VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (id, customer_id, created_at, text, ad_type, price, currency, payment_type, payment_cost))
    conn.commit()

def duplicate_check(id1):
    conn = connect_to_mysql()
    cursor = conn.cursor()
    cursor.execute("SELECT EXISTS(SELECT id from Classifieds WHERE id = %s)",id1)
    check = cursor.fetchall()
    return check[0][0]

def main():
    partition = TopicPartition(topic = 'data', partition = 0) 
    consumer = KafkaConsumer(group_id = 'data_group',
                         bootstrap_servers = ['15.188.249.229'],
                         enable_auto_commit = False, # commit manually to not lose any offsets in case of crash
                         heartbeat_interval_ms = 5000, # ensure that the consumer’s session stays active and rebalances when new consumers join or leave the group 
                         auto_offset_reset = 'earliest', # start messages from the top of the topic
                         session_timeout_ms = 1000 * 60 * 5 # long timeout timer in case of inactivity
                         )
    consumer.assign([partition]) #assign 1 partition
    for message in consumer:
        msg = ast.literal_eval(message.value.decode('utf-8'))
        id1 = msg['id']
        duplicate = duplicate_check(id1)
        if duplicate == 0:
            try:
                id = msg['id']
                customer_id = msg['customer_id']
                created_at = dateutil.parser.parse(msg['created_at'])
                text = msg['text']
                ad_type = msg['ad_type']
                price = msg['price']
                currency = msg['currency']
                payment_type = msg['payment_type']
                payment_cost = msg['payment_cost']        
            except KeyError:
                continue
            finally:
                if ad_type == 'Free':
                    add_new_event(id, customer_id, created_at, text, ad_type, None, None, None, None)
                    print("Event Added - Free Ad")
                    consumer.commit()
                else:
                    add_new_event(id, customer_id, created_at, text, ad_type, price, currency, payment_type, payment_cost)
                    print("Event Added - Not Free Ad")
                    consumer.commit()
        else:
            print("Event already commited")                    

if __name__ == "__main__":
    main()