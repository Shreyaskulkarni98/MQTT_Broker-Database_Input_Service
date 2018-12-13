import paho.mqtt.client as paho
import logging
import datetime
import json
# import random
import time

curr_date = str(datetime.datetime.now().strftime("%d_%b_%Y"))
logging.basicConfig(filename='Publisher_logging_' + curr_date + ".log", filemode='w', level=logging.INFO)

def on_connect(client, userdata, flags, rc):
    logging.info(str(datetime.datetime.now().strftime("%c")) + ' Broker is now connected')


def on_publish(client, userdata, mid):
    print("Message "+str(mid)+" sent")
    logging.info(str(datetime.datetime.now().strftime("%c")) + ' Broker has now published')


if __name__ == '__main__':
    client = paho.Client()
    client.on_connect = on_connect
    client.on_publish = on_publish
    client.connect("test.mosquitto.org", 1883, keepalive=60)
    # WORDS = ("Apples", "Bananas", "Carrots", "Oranges", "Mangos", "Cranberry")

    # JSON doc
    json_doc = {
        "time": "2018-12-10 16:45:21",
        "message_topic": "topic1/subtopic/sub2topic/sub3topic",
        "Qos": 1,
        "message_payload": "Mango"
    }, {
        "time": "2018-12-15 18:33:11",
        "message_topic": "topic1/subtopic/sub2topic/sub3topic",
        "Qos": 1,
        "message_payload": "Cranberry"
    }, {
        "time": "2018-08-15 13:05:44",
        "message_topic": "topic1/subtopic/sub2topic/sub3topic",
        "Qos": 1,
        "message_payload": "Apple"
    }, {
        "time": "2018-10-11 12:10:10",
        "message_topic": "topic1/subtopic/sub2topic/sub3topic",
        "Qos": 1,
        "message_payload": "Pine"
    }, {
        "time": "2018-01-11 11:11:11",
        "message_topic": "topic1/subtopic/sub2topic/sub3topic",
        "Qos": 1,
        "message_payload": "Banana"
    }

    client.loop_start()

    for x in json_doc:
        # word = random.choice(WORDS)
        client.publish(topic='f/f/f/f', payload=json.dumps(x), qos=1, retain=False)
        # Sleeping for 3 seconds
        time.sleep(3)

    client.loop_stop()
