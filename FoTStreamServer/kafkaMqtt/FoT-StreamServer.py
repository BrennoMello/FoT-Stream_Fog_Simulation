from confluent_kafka import Consumer, KafkaError
import time, json, dateutil.parser, datetime
from threading import Thread
import sys
import argparse
sys.path.insert(0, '/home/mininet/FoT-Simulation/FoTStreamServer/tsDeep')
sys.path.insert(0, '/home/mininet/FoT-Simulation/FoTStreamServer/conceptdrift/algorithms/')
import series
import adwin
#import pywt
import Wavelet
import threading
from memory_profiler import profile

############## Parse Arguments
parser = argparse.ArgumentParser(prog='FoT-StreamServer', usage='%(prog)s [options]', description='FoT-Stream Server')
parser.add_argument('-n','--name', type=str, help='Server Name',required=True)
parser.add_argument('-i','--ip', type=str, help='Server IP',required=True)
parser.add_argument('-p','--port', type=str, help='Server Port',required=True)

args = parser.parse_args()
kafka_local = args.ip+':'+args.port
kafka_remote = '18.218.147.104'
kafka_remote_port = '9092'
group = 'server'

objWavelet = Wavelet.Wavelet()

class SensorKafkaConsumer(object):
	kafka_consumer = None
	topic = ""
	group = ""
	Sensorid = ""
	DeviceId = ""
    
	
	def __init__ (self):
		global kafka_local, group
        #Thread.__init__(self)
        #self.topic = topic
        #self.group = group
        #self.Sensorid = sensorId
        #self.deviceId = deviceId
		print('Init FoT-Stream Server')
		conf = {'bootstrap.servers': kafka_local,
        'group.id': group,
        'auto.offset.reset': 'earliest', 'max.poll.interval.ms': 86400000,
        'heartbeat.interval.ms': 3600000}
		#self.kafka_consumer = Consumer(({'bootstrap.servers': kafka_local,'group.id': group,'auto.offset.reset': 'earliest'}))
		self.kafka_consumer = Consumer(conf)
		self.gatewaySensoresData = {}
		self.modelsLSTM = {}
		self.dicDetectors = {}
		self.inicializationModel = {}
		self.verifyTrain = {}
        ##self.loopKafka()
		
	@profile	
	def process_messages(self, jsonData):
		#{"gatewayID": "h1", "values": [172.7504292845607, 172.7504292], "name": "ufbaino15"}
		if jsonData["gatewayID"] in self.gatewaySensoresData:
			for value in jsonData["values"]:
				self.gatewaySensoresData[jsonData["gatewayID"]].append(value)
			print ("insert")
		else:
			self.gatewaySensoresData[jsonData["gatewayID"]] = []
			self.dicDetectors[jsonData["gatewayID"]] = adwin.ADWINChangeDetector()
			self.modelsLSTM[jsonData["gatewayID"]] = series.modelLSTM(jsonData["gatewayID"])
			self.inicializationModel[jsonData["gatewayID"]] = False
			#self.verifyTrain[jsonData["gatewayID"]] == False
			print ("new")	 
		
		print(self.gatewaySensoresData[jsonData["gatewayID"]])
		self.check_windows()
	
	@profile
	def train_lstm(self, data, indexGateway):
		self.modelsLSTM[indexGateway].create_model(data)
	
	@profile
	def check_windows(self):
		for indexGateway in self.gatewaySensoresData:
			#print len(sensoresData[indexSensor])
			if len(self.gatewaySensoresData[indexGateway]) >= 50:
				print ('------------------------  detecting concept drift ' +  indexGateway)
								
				print("Compĺete List")
				print(self.gatewaySensoresData[indexGateway])
				
				#cA, cD = pywt.dwt(self.gatewaySensoresData[indexGateway], 'haar')
				#print (cA)
				#print (cD)
				
				cA = objWavelet.pyramid(self.gatewaySensoresData[indexGateway],1)
				print("Wavelet Data")
				print(cA)
				
				listMessage = []
				trainLSTM = False
				detectChange = False
				for data in cA[1]:

					#listMessage.append(data)
					warning_status, detectChange = self.dicDetectors[indexGateway].run(data)
					if detectChange == True:
						trainLSTM = True
				
				if (trainLSTM == True or self.inicializationModel[indexGateway] == False):
					self.inicializationModel[indexGateway] = True
					print("Try Traning neural network Gateway " + indexGateway)
					self.modelsLSTM[indexGateway].create_model(cA[1])
					#if indexGateway not in self.verifyTrain:
						# get results from thread
						# print("Traning neural network Gateway " + indexGateway)
						# Create a Thread with a function without any arguments
						# th = threading.Thread(target=self.train_lstm, args=(cA[1], indexGateway))
						# Start the thread
						# th.start()
						# Wait for thread to finish
						# th.join()
						# self.verifyTrain[indexGateway] = th
					#elif not self.verifyTrain[indexGateway].isAlive():
						# get results from thread
						# print("Traning neural network Gateway " + indexGateway)
						# Create a Thread with a function without any arguments
						# th = threading.Thread(target=self.train_lstm, args=(cA[1], indexGateway))
						# Start the thread
						# th.start()
						# Wait for thread to finish
						# th.join()
						# self.verifyTrain[indexGateway] = th
					
				else:
					#if not self.verifyTrain[indexGateway].isAlive():
					print("No Train neural network and run RMSE Gateway " + indexGateway)
					self.modelsLSTM[indexGateway].calc_rmse(cA[1])		
					
				self.gatewaySensoresData[indexGateway] = []
	
	@profile
	def run(self):
		#consumer.subscribe(pattern='^awesome.*')
		
		self.kafka_consumer.subscribe(['dev'])
    
        #nome_arquivo = "Log-" + self.deviceId + "-" + self.Sensorid + "-" + str(datetime.datetime.now()) + ".txt"
        #arquivo = open(nome_arquivo, 'w+')  
		print('TimeOut '+str(sys.maxsize))
        # Now loop on the consumer to read messages
		running = True
		while running:
			
			
			#message = self.kafka_consumer.poll(timeout=sys.maxsize)
			message = self.kafka_consumer.poll()
            #arquivo = open(nome_arquivo, 'w+')
			if message is None:
				continue
            
			if message.error():
				print("Consumer error: {}".format(message.error()))
				continue

			print('Received message: {}'.format(message.value().decode('utf-8')))
            
			jsonData = json.loads(message.value().decode('utf-8'))
            
            
			self.process_messages(jsonData)
			    
            # agrupar por mote-id 
            # media vetorzao 
            # com haar e sem haar
            # se for incial rodar rede 
            # concept drift == True
            
				# entrada rede LSTM 
				# treinar rede
				# ficar testando previsao
			
			# concept drift == False
				
				# testando previsao
            
            # a hipotese com haar e com concept drift menos dados e com RMSE igual
            # vetor com janela aleatoria
            # 30 bacht 
            # media do rmse - trafego - 
            # decaimento da acuracia
            
            #parsedJson = json.loads(message.value().decode('utf-8'))
			
            #{"deviceId":"sc01","sensorId":"temperatureSensor","localDateTime":"2018-12-28T03:41:11.559","valueSensor":["30.87","30.42","30.19","30.23","30.35","30.1","30.06","30.41","30.54","30.6"],"delayFog":116,"WindowSize":201}
            #{"deviceId":"sc01","sensorId":"temperatureSensor","localDateTime":"2018-12-28T01:26:11.559"}
            	
            #latency = parsedJson['localDateTime']
            #parsed_data_latency = dateutil.parser.parse(latency)
            #print("Get Latency " + latency)
            #print("Get Latency Parsed " + str(abs(datetime.datetime.now()-parsed_data_latency).seconds))
            #parsedJson['LatencyWindow'] = str(abs(datetime.datetime.now()-parsed_data_latency).seconds)
            #print(parsedJson)
            #arquivo = open(nome_arquivo, 'a')
            #arquivo.write(json.dumps(parsedJson))
            #arquivo.write('\n')
            #arquivo.close()
		 
	
			 
#DevicesConnected = '[{"id":"sc02","latitude":57.290411,"longitude":-8.074406,"sensors":[{"id":"temperatureSensor","type":"ambientSensors","collection_time":30000,"publishing_time":60000},{"id":"luminositySensor","type":"ambientSensors","collection_time":30000,"publishing_time":60000},{"id":"dustSensor","type":"ambientSensors","collection_time":30000,"publishing_time":60000},{"id":"humiditySensor","type":"ambientSensors","collection_time":30000,"publishing_time":60000},{"id":"soundSensor","type":"ambientSensors","collection_time":30000,"publishing_time":60000}]},{"id":"libeliumSmartWater01","latitude":53.290411,"longitude":-9.074406,"sensors":[{"id":"TemperatureSensorPt-1000","type":"WaterMeter","collection_time":30000,"publishing_time":60000},{"id":"ConductivitySensor","type":"WaterMeter","collection_time":30000,"publishing_time":60000},{"id":"DissolvedOxygenSensor","type":"WaterMeter","collection_time":30000,"publishing_time":60000},{"id":"pHSensor","type":"WaterMeter","collection_time":30000,"publishing_time":60000},{"id":"OxidationReductionPotentialSensor","type":"WaterMeter","collection_time":30000,"publishing_time":60000},{"id":"TurbiditySensor","type":"WaterMeter","collection_time":30000,"publishing_time":60000}]},{"id":"libeliumSmartWaterIons01","latitude":57.290411,"longitude":-8.074406,"sensors":[{"id":"NitriteNO2Sensor","type":"WaterMeter","collection_time":30000,"publishing_time":60000}]},{"id":"sc01","latitude":57.290411,"longitude":-8.074406,"sensors":[{"id":"temperatureSensor","type":"ambientSensors","collection_time":30000,"publishing_time":60000},{"id":"luminositySensor","type":"ambientSensors","collection_time":30000,"publishing_time":60000},{"id":"dustSensor","type":"ambientSensors","collection_time":30000,"publishing_time":60000},{"id":"humiditySensor","type":"ambientSensors","collection_time":30000,"publishing_time":60000},{"id":"soundSensor","type":"ambientSensors","collection_time":30000,"publishing_time":60000}]}]'

#print(DevicesConnected)
#jsonDevicesConnected = json.loads(DevicesConnected)
#print(jsonDevicesConnected)

#for devices in jsonDevicesConnected:
#    if(devices['id']=='sc01' or devices['id']=='sc02' ):
#        for sensors in devices['sensors']:
#            print("dev."+devices['id']+"."+sensors['id'])
#            ConsumerSensorTemperature = SensorKafkaConsumer(devices['id'], sensors['id'],"dev."+devices['id']+"."+sensors['id'], "Fog-IME", "18.218.147.104:9092")
#            ConsumerSensorTemperature.start()
            
ConsumerSensor = SensorKafkaConsumer()
ConsumerSensor.run()
