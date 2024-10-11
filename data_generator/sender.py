import configparser
import boto3
import json
import time
from data_generator import generate_financial_transaction
from corruptors import corrupt_data

config = configparser.ConfigParser()
config.read('.\Config\conf.cfg')

delivery_stream_name = config['FIREHOSE']['STREAM_NAME']

AWS_ACCESS_KEY_ID = config['KEYS']['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = config['KEYS']['AWS_SECRET_ACCESS_KEY']

# Initialize Firehose client
firehose_client = boto3.client('firehose',
                                region_name='eu-central-1',
                                aws_access_key_id=AWS_ACCESS_KEY_ID,
                                aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

def send_data_to_firehose(records):
    """Sends the data to Firehose."""
    record_dicts = [{"Data": json.dumps(record)} for record in records]
    response = firehose_client.put_record_batch(
        DeliveryStreamName=delivery_stream_name,
        Records=record_dicts
    )
    return response

if __name__ == '__main__':
    
    while True:
        # Generate random financial data
        transaction_data = generate_financial_transaction(100)

        # Corrupt the data (simulate errors)
        corrupt_data(transaction_data)

        # Send the data to Firehose
        response = send_data_to_firehose(transaction_data)
        #print(f"Response: {response}")
        
        # Sleep to simulate continuous streaming, adjust as needed
        time.sleep(10)
