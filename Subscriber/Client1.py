import paho.mqtt.client as paho
import logging
import datetime

logging.basicConfig(filename='example.log', level=logging.INFO)

def on_subscribe(client, userdata, mid, granted_qos):
#    print("Subscribed: " + str(mid) + " " + str(granted_qos))
    logging.info(str(datetime.datetime.now().strftime("%c"))+" Client is now subscribed to broker")


def on_message(client, userdata, msg):
#    print msg
    print(msg.topic + " QoS Of Message=" + str(msg.qos) + " \nMessage: " + str(msg.payload))
    logging.info(str(datetime.datetime.now().strftime("%c"))+" Client has recieved a message")

if __name__ == '__main__':
    client = paho.Client()
    #Connecting to database
    conn = MySQLdb.connect('library', user='suhas', password='python')
    client.on_subscribe = on_subscribe
    logging.info("Before on message()")
    client.on_message = on_message

    #Connecting to broker
    client.connect("test.mosquitto.org", 1883)
    client.subscribe("topic1/subtopic/sub2topic/sub3topic", qos=1)
#    client.disconnect()
    client.loop_forever()