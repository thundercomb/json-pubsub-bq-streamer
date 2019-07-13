import time
import json

from google.cloud import pubsub_v1
from google.cloud import bigquery

project_id = "my_project"
subscription_name = "json_test"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_name)

bq_client = bigquery.Client()
dataset_id = 'languages'
table_id = 'latin'
table_ref = bq_client.dataset(dataset_id).table(table_id)
table = bq_client.get_table(table_ref)

def callback(message):
    if message.data:
        row = message.data.decode('utf-8')
        row_to_insert = json.loads(row)
        print(row_to_insert)
        errors = bq_client.insert_rows_json(table, row_to_insert)
        if errors != []:
            print(row_to_insert)
            print(errors)
    message.ack()
    assert errors == []

subscriber.subscribe(subscription_path, callback=callback)

# The subscriber is non-blocking, so we must keep the main thread from
# exiting to allow it to process messages in the background.
print('Listening for messages on {}'.format(subscription_path))
while True:
    time.sleep(10)
