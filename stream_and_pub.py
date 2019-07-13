from ijson import parse
from contextlib import closing
from urllib import request
import re

from google.cloud import pubsub

project_id = "my_project"
topic_name = "json_test"
url = "https://jsonplaceholder.typicode.com/posts"
publisher = pubsub.PublisherClient(batch_settings=pubsub.types.BatchSettings(max_latency=5))
topic_path = publisher.topic_path(project_id, topic_name)

f = request.urlopen(url)
parser = parse(f)

row = ''
for prefix, event, value in parser:
    if prefix == 'item.userId':
        row = row + "[{\"userId\":" + str(value) + ","
    if prefix == 'item.id':
        row = row + "\"id\":" + str(value) + ","
    if prefix == 'item.title':
        row = row + "\"title\":\"" + value + "\","
    if prefix == 'item.body':
        row = row + "\"body\":" + repr(value).replace("'","\"") + "}]"
        publisher.publish(topic_path, data=row.encode('utf-8'))
        row = ''
