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
parser = argparse.ArgumentParser(prog='virtual_device', usage='%(prog)s [options]', description='Virtual Device')
parser.add_argument('-n','--name', type=str, help='Device Name',required=True)
parser.add_argument('-s','--sensor', type=str, help='Sensor Name',required=True)
parser.add_argument('-p','--port', type=str, help='Broker Ports',required=True)
parser.add_argument('-i','--ip', type=str, help='Gateway IP',required=True)
args = parser.parse_args()

#################### GLOBAL VARS
broker=args.ip
portBroker = args.port
keepAliveBroker = 60
topicSubscribe = "dev/"+args.name
client =mqtt.Client(client_id='', clean_session=True, userdata=None, protocol=mqtt.MQTTv31)
tatu_message_type=""
publish_msg=0
collect_msg=0
thread_use=False


#json to Object
class to_object(object):
	def __init__(self, j):
		self.__dict__ = json.loads(j)

################# THREAD Flow Publish 
class Th(Thread):
	global publish_msg, sample, args, delta, data_samples, indice, samples_l
	
	def __init__ (self):
		Thread.__init__(self)
	
	def run(self):
		global tatu_message_type, thread_use
		while thread_use==True:
			if(tatu_message_type=="flow"):
				ini=timeit.default_timer()
				a=self.publish()
				fim=timeit.default_timer()
				time.sleep(float(self.get_time_publish())-float(fim-ini))
			elif(tatu_message_type=="evt" and delta!=0):
				pass
	
	def get_value(self):
		return str(int(random.randint(18,37)))
	
	def get_time_publish(self):
		global publish_msg
		return str(publish_msg/1000)

	def publish(self):
		global tatu_message_type, args, collect_msg, publish_msg
		
		if(tatu_message_type=="flow"):
			a= "{\"CODE\":\"POST\",\"METHOD\":\"FLOW\",\"HEADER\":{\"NAME\":\""+str(args.name)+"\"},\"BODY\":{\""+str(args.sensor)+"\":[\""+str(self.get_value())+"\"],\"FLOW\":{\"publish\":"+str(publish_msg)+",\"collect\":"+str(collect_msg)+"}}}"
			client.publish('dev/'+args.name,a)

def config_publish_collect(st,name):
	global publish_msg,collect_msg, args
	print "Init storage flow"
	st=st.replace('FLOW INFO '+args.sensor,'')
	st=st.replace("collect","\"collect\"")
	st=st.replace("publish","\"publish\"")
	name=name.replace('dev/','')
	ob=to_object(st)
	publish_msg=int(ob.publish)
	collect_msg=int(ob.collect)
		
def catch_message(msg,topic):
	global thread_use, tatu_message_type, args
	if(msg.find('FLOW INFO '+args.sensor+' {collect')==0 and topic.find('dev/'+args.name)==0):
		#iniciar flow via thread
		tatu_message_type="flow"
		thread_use=True
		config_publish_collect(msg,args.name)
		a = Th()
		a.daemon=True
		a.start()
	elif(msg.find('FLOW INFO '+args.sensor+' {turn:1}')==0 and topic.find('dev/'+name_device)==0):
		#finalizar flow
		thread_use=False
		


def on_connect(client, userdata, flags, rc):
	global topicSubscribe, args
    #automatic subscribe
	client.subscribe(topicSubscribe)
	
	
		

#Broker receiver
def on_message(client, userdata, msg):
	mensagemRecebida = str(msg.payload)
	#chegou msg
	#print ("On Message: ",mensagemRecebida," topic: ",msg.topic)
	catch_message(mensagemRecebida,str(msg.topic))
	
	
def config_mqtt():
	global client
	try:
			
			print("Init Virtual Device")
			
			client.on_connect = on_connect
			client.on_message = on_message

			client.connect(broker, portBroker, keepAliveBroker)
			client.loop_forever()
	except:
			print "\nCtrl+C leav..."
			sys.exit(0)
			
###########Main
config_mqtt()
