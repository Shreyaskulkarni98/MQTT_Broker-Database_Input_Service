import paho.mqtt.client as paho
import logging
import datetime
from elasticsearch import Elasticsearch

global nm
#curdtdate = str(datetime.datetime.now().strftime('x')) + '.log'
logging.basicConfig(filename='logging.log', filemode='w', level=logging.INFO)

# Connecting to es db
es = Elasticsearch()
logging.info(str(datetime.datetime.now().strftime("%c")) + ' Connected to a ElasticSearch Successfully')

def on_subscribe(client, userdata, mid, granted_qos):
    # print("Subscribed: " + str(mid) + " " + str(granted_qos))
    logging.info(str(datetime.datetime.now().strftime("%c")) + ' Client is now subscribed to broker')


def on_message(client, userdata, msg):
    print "Message recieved"
    # print(msg.topic + " QoS Of Message=" + str(msg.qos) + " \nMessage: " + str(msg.payload))
    logging.info(str(datetime.datetime.now().strftime("%c")) + " Client has recieved a message")

    # Inserting into es
    # print msg.topic + " " + msg.payload + " "
    es.index(index='fruits', doc_type='tweet', body=msg.payload)
    print "Inserted into ES"
    logging.info(str(datetime.datetime.now().strftime("%c")) + " Inserted into ES")

if __name__ == '__main__':
    client = paho.Client()
    nm = 0
    # Subscribing to a broker
    client.on_subscribe = on_subscribe
    logging.info("Before on message()")
    client.on_message = on_message

    # Connecting to broker
    client.connect("test.mosquitto.org", 1883)
    client.subscribe('f/f/f/f', qos=1)

    # Reopen this statement at the end of your code
    while not raw_input() == 'exit':
        client.loop()

    print("looped  out")
    res = es.search(index='fruits')
    print res['hits']

    # client.loop_stop()
    # client.disconnect()

