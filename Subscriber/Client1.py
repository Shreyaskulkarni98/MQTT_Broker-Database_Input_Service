import paho.mqtt.client as paho
import logging
import datetime
import MySQLdb
from elasticsearch import Elasticsearch

curdtname = str(datetime.datetime.now().strftime('c')) + '.log'
logging.basicConfig(filename='MySQL' + curdtname, filemode='w', level=logging.INFO)
logging.basicConfig(filename='ElasticSearch' + curdtname, filemode='w', level=logging.INFO)

try:
    # Connecting to mysql db
    conn = MySQLdb.connect(host='localhost', user='root', passwd='Kulkarni10')
    logging.info(str(datetime.datetime.now().strftime("%c")) + ' Connected to a MySQL DB Successfully')
    curs = conn.cursor()
    # Connecting to es db
    es = Elasticsearch()
    logging.info(str(datetime.datetime.now().strftime("%c")) + ' Connected to a ElasticSearch Successfully')

    def on_subscribe(client, userdata, mid, granted_qos):
        # print("Subscribed: " + str(mid) + " " + str(granted_qos))
        logging.info(str(datetime.datetime.now().strftime("%c")) + ' Client is now subscribed to broker')

        # Defining a mysql db
        curs.execute('use test_database')

        # Defining a es index



    def on_message(client, userdata, msg):
        print "Message recieved"
        # print(msg.topic + " QoS Of Message=" + str(msg.qos) + " \nMessage: " + str(msg.payload))
        logging.info(str(datetime.datetime.now().strftime("%c")) + " Client has recieved a message")

        try:
            curs.execute("insert into mqttbroker values(now(),%s,%s,%s)", [str(msg.topic), str(msg.qos), str(msg.payload)])
            conn.commit()
        except(MySQLdb.Error, MySQLdb.Warning) as e:
            print(e)

        logging.info(str(datetime.datetime.now().strftime("%c")) + " Message has been committed to database")


    if __name__ == '__main__':
        client = paho.Client()
        # Subscribing to a broker
        client.on_subscribe = on_subscribe
        logging.info("Before on message()")
        client.on_message = on_message

        # Connecting to broker
        client.connect("test.mosquitto.org", 1883)
        client.subscribe("topic1/subtopic/sub2topic/sub3topic", qos=1)

        while not raw_input() == 'exit':
            client.loop()

        client.loop_stop()
        client.disconnect()

finally:
    conn.close()

