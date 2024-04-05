'''=================================================
 Visual in mqtt тестовая #4.0 многомашинная
 Публикация при изменении файла send.byn
 Tопик 05XXXXTTTT, error code
 c перезапуском при отключении интернета
 broker belaz (м) OIM 02.04.24
====================================================='''

import time
import datetime
import paho.mqtt.client as mqtt
import socket
import gzip
import sys
import os
import subprocess

#MQTT_ADDR = "broker.hivemq.com"
#MQTT_ADDR = "test.mosquitto.org"
#MQTT_ADDR = "mqtt.eclipseprojects.io"
MQTT_ADDR = '178.124.155.107' # "belaz"

MQTT_PORT = 1883
MQTT_USER = 'vibro'
MQTT_PASS = 'vibroaccess'
MQTT_TOPI = '/vibro/'

def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))

def on_disconnect(client, userdata, rc):
    if rc != 0:
        print("Unexpected disconnection. Restarting program in 1 minutes...")
        time.sleep(60)  # Подождать 1 минуту перед перезапуском программы
        subprocess.run([sys.executable] + sys.argv)  # Перезапустить из текущего экземпляра        

def publish(message):
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.username_pw_set(MQTT_USER, MQTT_PASS)
    #client.connect(MQTT_ADDR, MQTT_PORT, 60)
    try:
        client.connect(MQTT_ADDR, MQTT_PORT, 60) # установка связи
        #print("Connect!")
    except (socket.error, OSError) as e:
        print("Connection error. Restarting...")
        subprocess.run([sys.executable] + sys.argv)
        
    client.loop_start()   
    
    unit=((message)[1:5]) #yes          
    int_val = int.from_bytes((message)[1:5], "little")
    
    units='0'+hex(int_val)[2:9]
    
    x = datetime.datetime.now()
    print(x.strftime("%X"), end=" ")
    print("05 ", end=" ")
    print(int_val, end=" ")
    
    tt = time.time()             
    topic = "/vibro/00000005"+units+hex(int(tt))[2:10] # топик
    num_times = int(tt).to_bytes(8, byteorder='big') # integer to bytes
           
    lc=list(message)
    lc[0]=8
    lc[5]= num_times[0]
    lc[6]= num_times[1]
    lc[7]= num_times[2]
    lc[8]= num_times[3]
    lc[9]= num_times[4]
    lc[10]= num_times[5]
    lc[11]= num_times[6]
    lc[12]= num_times[7]
    lc[13]=5
    lc[14]=0
    lc[15]=21     

    message = bytes (lc)                    
    message = gzip.compress(message) # отладка
    try:
        client.publish(topic, message) # публикация
        print("OK.")
    except (socket.error, OSError) as e:
        print("Connection error. Restarting ...")
        subprocess.run([sys.executable] + sys.argv)  # Перезапустить из текущего экземпляра    
                     
    client.disconnect()
    
#main()
print("MQTT_visual 4.0 to "+ MQTT_ADDR)

path = "c:\database\send.bin"
last = os.path.getmtime(path)

while True:
    current= os.path.getmtime(path)
    if current != last:
    #if 1==1: #debug
        #time.sleep(5) 
        with open(path, "rb") as file:
            contents = file.read()
            
            publish(contents)
            #print("Публикация...")
            
        last= current
    time.sleep(1)  # Пауза в 1 секунду 

client.loop_stop()
client.disconnect()