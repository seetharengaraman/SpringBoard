"""Determine if transactions are fraudulent, add them to a file.
When file size exceeds an amount, send notification email
as escalation"""
import os
import json
from sys import api_version
from kafka import KafkaConsumer, KafkaProducer
from send_mail import gmail_send_message

KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL")
TRANSACTIONS_TOPIC = os.environ.get("TRANSACTIONS_TOPIC")
LEGIT_TOPIC = os.environ.get("LEGIT_TOPIC")
FRAUD_TOPIC = os.environ.get("FRAUD_TOPIC")

def is_suspicious(transaction: dict) -> bool:
    """Determine whether a transaction is suspicious."""
    return transaction["amount"] >= 900

if __name__ == "__main__":
    consumer = KafkaConsumer(
                            TRANSACTIONS_TOPIC,
                            bootstrap_servers=KAFKA_BROKER_URL,
                            value_deserializer=lambda value: json.loads(value)
                            )
    producer = KafkaProducer(
                            bootstrap_servers=KAFKA_BROKER_URL,
                            value_serializer=lambda value: json.dumps(value).encode(),
                            api_version=(2,0,2)
                            )
    file_size = 0
    for message in consumer:
        transaction: dict = message.value
        topic = FRAUD_TOPIC if is_suspicious(transaction) else LEGIT_TOPIC
        if topic == FRAUD_TOPIC:
            email_message = f"Origin Account:{transaction['source']} \
                Target Account:{transaction['target']} Amount:{transaction['amount']} USD\n"
            print(email_message)
            with open('fraud_transactions.txt','a') as f:
                f.writelines(email_message)
            file_size = os.stat('fraud_transactions.txt').st_size   
            print(f'File Size: {file_size}') 
            if file_size > 10000:
                break        
        producer.send(topic, value=transaction)
        print(topic,transaction)
    gmail_send_message('fraud_transactions.txt')