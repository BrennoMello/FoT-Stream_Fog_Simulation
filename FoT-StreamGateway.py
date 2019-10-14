import paho.mqtt.client as mqtt
import sys
import json
import time
import timeit
import random
from threading import Thread
import argparse
import fileinput
import data_set 
from FoTStreamServer.conceptdrift.algorithms import cusum
from FoTStreamServer.conceptdrift.algorithms import adwin
from traceback import print_exc
from kafka import KafkaProducer
#import pywt
from FoTStreamServer.kafkaMqtt import Wavelet

############## Parse Arguments
parser = argparse.ArgumentParser(prog='FoT-StreamGateway', usage='%(prog)s [options]', description='FoT-Stream Gateway')
parser.add_argument('-n','--name', type=str, help='Gateway Name',required=True)
parser.add_argument('-i','--ip', type=str, help='Server IP',required=True)
parser.add_argument('-p','--port', type=str, help='Broker Ports',required=True)

args = parser.parse_args()

brokerMQTT = 'localhost'
portBrokerMQTT = 1883
kafka_ServerIp = args.ip+':'+args.port
#kafka_ServerPort = args.port
keepAliveBrokerMQTT = 60
client = mqtt.Client(client_id = '', clean_session=True, userdata=None, protocol = mqtt.MQTTv31)
producerKafka = KafkaProducer(bootstrap_servers=kafka_ServerIp)

#wavelet = pywt.Wavelet('haar')

sensoresData = {}
dicDetectors = {}
objWavelet = Wavelet.Wavelet()

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
	#print (msg.topic + "/" + str(v))
	#print ("New Message")
	try:
		#print (msg.payload)
		dataRaw = json.loads(msg.payload)
		value = parser_msg_value(dataRaw)
		nameSensor = parser_sensor_name(dataRaw)
		
		insert_window(nameSensor, value)
		check_windows()
		
		#print value
		#print nameSensor
	except Exception as inst:
		print_exc()
		print(inst)

def insert_window(sensor, data):
	global sensoresData
	try:
		#print (sensoresData.get[sensor])
		
		if sensor in sensoresData:
			sensoresData[sensor].append(data)
			#print "insert"
		else:
			sensoresData[sensor] = []
			dicDetectors[sensor] = adwin.ADWINChangeDetector()
			#print "new"
		
		#dicDetectors[sensor].run(data)
		
		#print dicDetectors[sensor].get_settings()
		
		check_windows()
	
	except Exception as inst:
		print_exc()
		print(inst)
	
	

def check_windows():
	for indexSensor in sensoresData:
		#print len(sensoresData[indexSensor])
		if len(sensoresData[indexSensor]) >= 30:
			print 'detecting concept drift ' +  indexSensor
			detectChange = False
			
			print("Complete List")
			print(sensoresData[indexSensor])
			
			#cA, cD = pywt.dwt(sensoresData[indexSensor], 'haar')
			#print (cA)
			#print (cD)
			
			cA = objWavelet.pyramid(sensoresData[indexSensor],1)
			print("Wavelet Data")
			print(cA)
			
			listMessageKafkaCa = []
			sendMessaKafka = False
			for data in cA[1]:
				
				listMessageKafkaCa.append(data)
				warning_status, detectChange = dicDetectors[indexSensor].run(data)
				if detectChange == True:
					sendMessaKafka = True
			
			
			if sendMessaKafka == True:			
				print 'send message kafka server ' + indexSensor
				json_message = {'gatewayID':args.name ,'name':indexSensor, 'values':listMessageKafkaCa}
				print(json_message)
				json_array = json.dumps(json_message)
				print(json_array)
				producerKafka.send('dev', str(json_array))
			else:
				print 'no drift detect ' + indexSensor
			#if detectChange == False:
			sensoresData[indexSensor] = []
			
				
				
				
	return None



def parser_sensor_name(data):
	if(str(data).find('BODY') != -1):
		try:
				
			header = data['HEADER']
			name = header['NAME']
	
			return name
		except Exception as inst:
			print(inst)


def parser_msg_value(data):
	if(str(data).find('BODY') != -1):
		try:
				
			body = data['BODY']
			value = body['temperatureSensor']
	
			return float(value[0])
		except Exception as inst:
			print(inst)
		

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
