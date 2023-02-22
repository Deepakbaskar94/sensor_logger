import paho.mqtt.client as mqtt
import sys
import time
import mysql.connector
import threading
import logging
from queue import Queue
q=Queue()

# def onMessage(client, userdata, msg):
#     #print(msg.topic + " : " + msg.payload.decode())
#     print(msg.payload.decode())


last_message = dict()

broker="35.154.235.247"
port=1883
keepalive = 60
verbose=True
cname="cloud_database"
username="probeplus"
password="$0nar1ta$"
topics="test/status"
Table_name = "sensorlog"




host = "localhost"
username = "root"
password = "Root123$"
db = "ppd"


#callbacks -all others define in functions module
def on_connect(client, userdata, flags, rc):
    print("rc: ", rc)
    logging.debug("Connected flags"+str(flags)+"result code " +str(rc)+"client1_id")
    if rc==0:
        client.connected_flag=True
    else:
        print("Bad Connection")
        client.bad_connection_flag=True

def on_disconnect(client, userdata, rc):
    logging.debug("disconnecting reason  " + str(rc))
    client.connected_flag=False
    client.disconnect_flag=True
    client.subscribe_flag=False
    
def on_subscribe(client,userdata,mid,granted_qos):
    m="in on subscribe callback result "+str(mid)
    print("subscribed")
    logging.debug(m)
    client.subscribed_flag=True


def on_message(client,userdata, msg):
    topic=msg.topic
    m_decode=str(msg.payload.decode("utf-8","ignore"))
    message_handler(client,m_decode,topic)
    print("message received")

def message_handler(client,msg,topic):
    data=dict()
    tnow=time.localtime(time.time())
    m=time.asctime(tnow)+" "+topic+" "+msg
    data["time"]=int(time.time())
    data["topic"]=topic
    data["message"]=msg
    data["sensor"]="auto_generated"
    # if has_changed(topic,msg):
        #print("storing changed data",topic, "   ",msg)
        # q.put(data) #put messages on queue
    print("putting in queue")
    q.put(data)

# def has_changed(topic,msg):
#     topic2=topic.lower()
#     if topic in last_message:
#         if last_message[topic]==msg:
#             return False
#     last_message[topic]=msg



def Initialise_clients(cname,cleansession=True):
    #flags set
    client= mqtt.Client(cname)
    # if mqttclient_log: #enable mqqt client logging
    #     client.on_log=on_log
    client.on_connect= on_connect        #attach function to callback
    client.on_message=on_message        #attach function to callback
    client.on_disconnect=on_disconnect
    client.on_subscribe=on_subscribe
    return client




#function to create connection
def create_connection(host_name, user_name, user_password, database):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            db=database
        )
        print("Connection to MySQL DB successful")
    except mysql.connector.Error as e:
        data = str(e)
        result = {}
        result["status"]= "failure"
        result["error"]= data
        return result

    return connection

#function to execute query to insert, update, delete data from variable
def execute_query_variable(connection, query, val):
    connection = create_connection(host, username,password,db)
    cursor = connection.cursor()
    try:
        cursor.execute(query, val)
        connection.commit()
        data = cursor.rowcount
        result = {}
        if data == 1:
            result["status"]= "success"
        else:
            result["status"]= "failure"
            
        cursor.close()
        connection.close()
        return result
    except mysql.connector.Error as e:
        data = str(e)
        result = {}
        result["status"]= "failure"
        result["error"]= data
        cursor.close()
        connection.close()
        return result

def log_worker():
    """runs in own thread to log data"""
    #create logger
    # logger=SQL_data_logger(db_file)
    # logger.drop_table("logs")
    # logger.create_table("logs",table_fields)
    while Log_worker_flag:
        time.sleep(0.01)
        while not q.empty():
            data = q.get()
            if data is None:
                continue
            try:
                timestamp=data["time"]
                topic=data["topic"]
                message=data["message"]
                sensor="Dummy-sensor"
                data_out=[timestamp,topic,sensor,message]
                data_query="INSERT INTO "+ \
                Table_name +"(time_data,topic,sensor,message)VALUES(?,?,?,?)"   
                execute_query_variable(data_query,data_out)
                print("added")
            except Exception as e:
                print("problem with logging ",e)
    # logger.conn.close()



#Initialise_client_object() # add extra flags
logging.info("creating client "+cname)
client=Initialise_clients(cname,False)#create and initialise client object
if username !="":
    print("setting username")
    client.username_pw_set(username, password)
print("starting")

##
t = threading.Thread(target=log_worker) #start logger
Log_worker_flag=True
t.start() #start logging thread
###




client.connected_flag=False # flag for connection
client.bad_connection_flag=False
client.subscribed_flag=False
client.loop_start()
check1 = 0
check1 = client.connect(broker,port,keepalive)
print("connect check1:", check1)

while not client.connected_flag: #wait for connection
    time.sleep(1)
print("trying to subscribe")
client.subscribe(topics)
while not client.subscribed_flag: #wait for connection
    time.sleep(1)
    print("waiting for subscribe")
print("subscribed ",topics)


#loop and wait until interrupted
try:
    while True:
        pass
except KeyboardInterrupt:
    print("interrrupted by keyboard")


client.loop_stop()  #final check for messages
time.sleep(5)
Log_worker_flag=False #stop logging thread
print("ending ")











# client = paho.Client()
# client.username_pw_set("probeplus", "$0nar1ta$")
# client.on_message = onMessage

# if client.connect("35.154.235.247", 1883, 60) != 0:
#     print("cannot able to connect to MQTT Broker")
#     sys.exit(-1)

# client.subscribe("test/status")

# try:
#     print("Pess CTRL+C to exit...")
#     client.loop_forever()
# except:
#     print("Disconnecting from broker")

# client.disconnect()
