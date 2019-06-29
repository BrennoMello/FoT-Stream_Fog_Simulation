import json, time
from random import randint
from kafka import KafkaProducer
from threading import Thread

class FogKafkaProducer(Thread):
		clientKafka = None
		kafka_address = ""
		fogId = ""
		jsonGatewaysConnected = {}
		producer = None
		def __init__ (self , fogId, kafka_address, jsonGatewaysConnected):
				Thread.__init__(self)
				self.fogId = fogId
				self.kafka_address = kafka_address
				self.jsonGatewaysConnected = jsonGatewaysConnected
				self.producer = KafkaProducer(bootstrap_servers=self.kafka_address)
				print(self.fogId)
       

		def run(self):
			running = True
			while running:      
				for gateway in self.jsonGatewaysConnected:
						message = {
							"delayFog": 194, "LatencyWindow": "197", 
							"WindowSize": 200, "deviceId": "sc01", 
  						    "localDateTime": "2019-01-17T16:46:07.508", 
							"sensorId": "dustSensor",
							"type": "dust",
   						    "valueSensor": ["-45.215", "46.925", "0.855", "17.04", "19.115", "4.59", "16.625", "53.98", "16.21", "15.38"]}
     
						#if(sensors['id']=='luminositySensor'):    
						topic='dev.'+gateway['id']+'.'+'temperatureSensor'
						print(topic)
						#data=randint(14, 30)
						#print(data)
						self.producer.send(topic, str(message))
						time.sleep(2)






broker_address="18.218.147.104:9092" 

#DevicesConnected = '[{"id":"sc02","latitude":57.290411,"longitude":-8.074406,"sensors":[{"id":"temperatureSensor","type":"ambientSensors","collection_time":30000,"publishing_time":60000},{"id":"luminositySensor","type":"ambientSensors","collection_time":30000,"publishing_time":60000},{"id":"dustSensor","type":"ambientSensors","collection_time":30000,"publishing_time":60000},{"id":"humiditySensor","type":"ambientSensors","collection_time":30000,"publishing_time":60000},{"id":"soundSensor","type":"ambientSensors","collection_time":30000,"publishing_time":60000}]},{"id":"libeliumSmartWater01","latitude":53.290411,"longitude":-9.074406,"sensors":[{"id":"TemperatureSensorPt-1000","type":"WaterMeter","collection_time":30000,"publishing_time":60000},{"id":"ConductivitySensor","type":"WaterMeter","collection_time":30000,"publishing_time":60000},{"id":"DissolvedOxygenSensor","type":"WaterMeter","collection_time":30000,"publishing_time":60000},{"id":"pHSensor","type":"WaterMeter","collection_time":30000,"publishing_time":60000},{"id":"OxidationReductionPotentialSensor","type":"WaterMeter","collection_time":30000,"publishing_time":60000},{"id":"TurbiditySensor","type":"WaterMeter","collection_time":30000,"publishing_time":60000}]},{"id":"libeliumSmartWaterIons01","latitude":57.290411,"longitude":-8.074406,"sensors":[{"id":"NitriteNO2Sensor","type":"WaterMeter","collection_time":30000,"publishing_time":60000}]},{"id":"sc01","latitude":57.290411,"longitude":-8.074406,"sensors":[{"id":"temperatureSensor","type":"ambientSensors","collection_time":30000,"publishing_time":60000},{"id":"luminositySensor","type":"ambientSensors","collection_time":30000,"publishing_time":60000},{"id":"dustSensor","type":"ambientSensors","collection_time":30000,"publishing_time":60000},{"id":"humiditySensor","type":"ambientSensors","collection_time":30000,"publishing_time":60000},{"id":"soundSensor","type":"ambientSensors","collection_time":30000,"publishing_time":60000}]}]'

FogsManagement = '[{"id":"FogA","type":"SmartBuilding","latitude":53.290411,"longitude":-9.074406,"gateways":[{"id":"Gateway01","type":"WaterMeter","collection_time":30000,"publishing_time":60000,"latitude":53.290411,"longitude":-9.074406},{"id":"Gateway04","type":"WaterMeter","collection_time":30000,"publishing_time":60000,"latitude":53.290411,"longitude":-9.074406 }]},{"id":"FogIME","type":"SmartBuilding","latitude":53.290411,"longitude":-9.074406,"gateways":[{"id":"Gateway02","type":"WaterMeter","collection_time":30000,"publishing_time":60000,"latitude":53.290411,"longitude":-9.074406},{"id":"Gateway03","type":"WaterMeter","collection_time":30000,"publishing_time":60000,"latitude":53.290411,"longitude":-9.074406}]}]'

jsonFogsConnected = None

#print(DevicesConnected)
jsonFogsConnected = json.loads(FogsManagement)
#print(jsonDevicesConnected)

for fog in jsonFogsConnected:
		#if(devices['id']=='sc01' or devices['id']=='sc02'):
		#if(devices['id']=='sc02'):
		#for gateway in fog['gateways']:
		#print("Fog " + gateway['id'])
		fogKafkaProducer = FogKafkaProducer(fog['id'], broker_address, fog['gateways'])
		fogKafkaProducer.start()



