# JSON Streamer with PubSub and BigQuery

Stream JSON data via PubSub into BigQuery using simple Python scripts and the Google Cloud libaries for PubSub and Bigquery. These scripts do not use a GCP service account. They assume that application default credentials are in use.

```
gcloud auth application-default login
```
  
For best results use Python 3.

# Install dependencies

```
pip3 install requirements.txt
```

# Stream to PubSub

```
python3 stream_and_pub.py
```

# Subscribe and insert into BigQuery

```
python3 sub_and_insert.py
```
