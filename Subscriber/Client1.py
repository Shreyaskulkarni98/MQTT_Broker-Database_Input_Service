import paho.mqtt.client as paho
import logging
import datetime
import MySQLdb

logging.basicConfig(filename='example.log', filemode='w', level=logging.INFO)

try:
    conn = MySQLdb.connect(host='localhost', user='root', passwd='Kulkarni10')
    curs = conn.cursor()

    def on_subscribe(client, userdata, mid, granted_qos):
    #    print("Subscribed: " + str(mid) + " " + str(granted_qos))
        logging.info(str(datetime.datetime.now().strftime("%c")) + ' Client is now subscribed to broker')
        curs.execute('use test_database')


    def on_message(client, userdata, msg):
        print "Message recieved"
    #    print(msg.topic + " QoS Of Message=" + str(msg.qos) + " \nMessage: " + str(msg.payload))
        logging.info(str(datetime.datetime.now().strftime("%c")) + " Client has recieved a message")

        try:
            curs.execute("insert into mqttbroker values(now(),%s,%s,%s)", [str(msg.topic), str(msg.qos), str(msg.payload)])
            conn.commit()
        except(MySQLdb.Error, MySQLdb.Warning) as e:
            print(e)

        logging.info(str(datetime.datetime.now().strftime("%c")) + " Message has been committed to database")


    if __name__ == '__main__':
        client = paho.Client()
        #Subscribing to a broker
        client.on_subscribe = on_subscribe
        logging.info("Before on message()")
        client.on_message = on_message

        #Connecting to broker
        client.connect("test.mosquitto.org", 1883)
        client.subscribe("topic1/subtopic/sub2topic/sub3topic", qos=1)

        while not raw_input()=='exit':
            client.loop()

    client.loop_stop()
    client.disconnect()

finally:
    conn.close()

