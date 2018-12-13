import paho.mqtt.client as paho
import logging
import datetime
import elasticsearch
from elasticsearch import Elasticsearch

curr_date = str(datetime.datetime.now().strftime("%d_%b_%Y"))
logging.basicConfig(filename='client_logging_' + curr_date + ".log", filemode='w', level=logging.INFO)

# Connecting to es db
es = Elasticsearch()
logging.info(str(datetime.datetime.now().strftime("%c")) + ' Connected to a ElasticSearch Successfully')

def on_subscribe(client, userdata, mid, granted_qos):
    logging.info(str(datetime.datetime.now().strftime("%c")) + ' Client is now subscribed to broker')


def on_message(client, userdata, msg):
    print "Message recieved"
    logging.info(str(datetime.datetime.now().strftime("%c")) + " Client has recieved a message")
    # Inserting into es
    try:
        es.index(index='fruits', doc_type='tweet', body=str(msg.payload))
        logging.info(str(datetime.datetime.now().strftime("%c")) + " Inserted into ES")
    except elasticsearch.ElasticsearchException as e1:
        print e1.info['error']['caused_by']['reason']


if __name__ == '__main__':
    client = paho.Client()

    # Subscribing to a broker
    client.on_subscribe = on_subscribe
    logging.info("Before on message()")
    client.on_message = on_message

    # Connecting to broker
    client.connect("test.mosquitto.org", 1883)
    client.subscribe('f/f/f/f', qos=1)

    while not raw_input() == 'exit':
        client.loop()

    res = es.search(index='fruits')
    print("Got %d Hits:" % res['hits']['total'])
    for hit in res['hits']['hits']:
        print(hit["_source"])

    client.loop_stop()
    client.disconnect()

