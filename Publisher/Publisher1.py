import paho.mqtt.client as paho
import logging
import datetime
import random
import time

logging.basicConfig(filename='example.log',filemode='w' , level=logging.INFO)

def on_connect(client, userdata, flags, rc):
#    print "CONNACK received with code % d." % (rc)
    logging.info(str(datetime.datetime.now().strftime("%c")) + ' Broker is now connected')

def on_publish(client, userdata, mid):
    print("Message "+str(mid)+" sent")
    logging.info(str(datetime.datetime.now().strftime("%c")) + ' Broker has now published')

if __name__ == '__main__':
    client = paho.Client()
    client.on_connect = on_connect
    client.on_publish = on_publish
    client.connect("test.mosquitto.org", 1883, keepalive=60)
    WORDS = ("Apples", "Bananas", "Carrots", "Oranges", "Mangos", "Cranberry")
    client.loop_start()
    for x in range(0,5):
        word = random.choice(WORDS)
        client.publish("topic1/subtopic/sub2topic/sub3topic", word, qos=1, retain=False)
        time.sleep(3)
    client.loop_stop()