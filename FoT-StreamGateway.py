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
parser.add_argument('-i','--ip', type=str, help='Server IP',required=True)
parser.add_argument('-p','--port', type=str, help='Broker Ports',required=True)

args = parser.parse_args()

brokerMQTT = 'localhost'
portBrokerMQTT = 1883
kafka_ServerIp=args.ip
kafka_ServerPort = args.port
keepAliveBrokerMQTT = 60
client = mqtt.Client(client_id = '', clean_session=True, userdata=None, protocol = mqtt.MQTTv31)

# funcao chamada quando a conexao for realizada, sendo
# entao realizada a subscricao
def on_connect(client, userdata, flags, rc):
	try:
		client.subscribe("dev/#")
	except Exception as inst:
		print(inst)
		
#{"CODE":"POST","METHOD":"FLOW","HEADER":{"NAME":"ufbaino16"},"BODY":{"temperatureSensor":["19.9884"],"FLOW":{"publish":1000,"collect":1000}}}
# funcao chamada quando uma nova mensagem do topico eh gerada
def on_message(client, userdata, msg):
    # decodificando o valor recebido
    #v = unpack(">H",msg.payload)[0]
    #print msg.topic + "/" + str(v)
	try:
		#message = str(msg.payload)
		#print (msg.payload)
		parser_msg(msg.payload)
	except Exception as inst:
		print(inst)

def parser_msg(msg):
	if(msg.find('BODY') != -1):
		try:
			#print json.loads(msg)
			msg = to_object(msg)
			print msg.BODY['temperatureSensor']
			value = msg.BODY['temperatureSensor']
			
			print float(str(value))
		except Exception as inst:
			print(inst)
		

def create_window(sensor, data):
	return None

class to_object(object):
	def __init__(self, j):
		self.__dict__ = json.loads(j)

def config_mqtt():
	global brokerMQTT, portBrokerMQTT, keepAliveBrokerMQTT, client
	try:
			
		print("Init FoT-Stream Gateway")
		#cria um cliente para supervisao
			
		client.on_connect = on_connect
		client.on_message = on_message
		client.connect(brokerMQTT, portBrokerMQTT, keepAliveBrokerMQTT)
		client.loop_forever()
        
	except Exception as inst:
		print(inst)
		print "\nCtrl+C leav..."
		sys.exit(0)
			
###########Main
config_mqtt()
