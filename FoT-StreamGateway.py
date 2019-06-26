import paho.mqtt.client as mqtt
import sys
import json
import time
import timeit
import random
from threading import Thread
import argparse
import fileinput


############## Parse Arguments
parser = argparse.ArgumentParser(prog='FoT-StreamGateway', usage='%(prog)s [options]', description='FoT-Stream Gateway')
parser.add_argument('-n','--name', type=str, help='Gateway Name',required=True)
parser.add_argument('-p','--port', type=str, help='Broker Ports',required=True)
parser.add_argument('-i','--ip', type=str, help='Server IP',required=True)
args = parser.parse_args()

broker=args.ip
portBroker = args.port
keepAliveBroker = 60


# função chamada quando a conexão for realizada, sendo
# então realizada a subscrição
def on_connect(client, data, rc):
    client.subscribe("dev/#")

# função chamada quando uma nova mensagem do tópico é gerada
def on_message(client, userdata, msg):
    # decodificando o valor recebido
    #v = unpack(">H",msg.payload)[0]
    #print msg.topic + "/" + str(v)
    print msg

def config_mqtt():
    global broker, portBroker, keepAliveBroker
    try:
			
        print("Init FoT-Stream Gateway")
		#clia um cliente para supervisã0
        client = mqtt.Client(client_id = '', clean_session=True, userdata=None, protocol = mqtt.MQTTv31)
			
        client.on_connect = on_connect
        client.on_message = on_message
        client.connect(broker, portBroker, keepAliveBroker)
        client.loop_forever()

    except:
		print "\nCtrl+C leav..."
		sys.exit(0)
			
###########Main
config_mqtt()
