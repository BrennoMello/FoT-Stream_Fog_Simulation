from confluent_kafka import Consumer, KafkaError
import time, json, dateutil.parser, datetime
from threading import Thread

class SensorKafkaConsumer(Thread):
    kafka_consumer = None
    topic = ""
    group = ""
    Sensorid = ""
    DeviceId = ""
    def __init__ (self , deviceId, sensorId ,topic, group, broker_server):
        Thread.__init__(self)
        self.topic = topic
        self.group = group
        self.Sensorid = sensorId
        self.deviceId = deviceId
        self.kafka_consumer = Consumer(({
                             'bootstrap.servers': broker_server,
                             'group.id': group,
                             'auto.offset.reset': 'earliest'
                            }))
        ##self.loopKafka()

    def run(self):
        self.kafka_consumer.subscribe([self.topic])
    
        nome_arquivo = "Log-" + self.deviceId + "-" + self.Sensorid + "-" + str(datetime.datetime.now()) + ".txt"
        arquivo = open(nome_arquivo, 'w+')  
        

        # Now loop on the consumer to read messages
        running = True
        while running:
            message = self.kafka_consumer.poll()
            #arquivo = open(nome_arquivo, 'w+')
            if message is None:
                continue
            
            if message.error():
                print("Consumer error: {}".format(message.error()))
                continue

            print('Received message: {}'.format(message.value().decode('utf-8')))
            parsedJson = json.loads(message.value().decode('utf-8'))

            #{"deviceId":"sc01","sensorId":"temperatureSensor","localDateTime":"2018-12-28T03:41:11.559","valueSensor":["30.87","30.42","30.19","30.23","30.35","30.1","30.06","30.41","30.54","30.6"],"delayFog":116,"WindowSize":201}
            #{"deviceId":"sc01","sensorId":"temperatureSensor","localDateTime":"2018-12-28T01:26:11.559"}
            	
            latency = parsedJson['localDateTime']
            parsed_data_latency = dateutil.parser.parse(latency)
            print("Get Latency " + latency)
            print("Get Latency Parsed " + str(abs(datetime.datetime.now()-parsed_data_latency).seconds))
            parsedJson['LatencyWindow'] = str(abs(datetime.datetime.now()-parsed_data_latency).seconds)
            print(parsedJson)
            arquivo = open(nome_arquivo, 'a')
            arquivo.write(json.dumps(parsedJson))
            arquivo.write('\n')
            arquivo.close()

DevicesConnected = '[{"id":"sc02","latitude":57.290411,"longitude":-8.074406,"sensors":[{"id":"temperatureSensor","type":"ambientSensors","collection_time":30000,"publishing_time":60000},{"id":"luminositySensor","type":"ambientSensors","collection_time":30000,"publishing_time":60000},{"id":"dustSensor","type":"ambientSensors","collection_time":30000,"publishing_time":60000},{"id":"humiditySensor","type":"ambientSensors","collection_time":30000,"publishing_time":60000},{"id":"soundSensor","type":"ambientSensors","collection_time":30000,"publishing_time":60000}]},{"id":"libeliumSmartWater01","latitude":53.290411,"longitude":-9.074406,"sensors":[{"id":"TemperatureSensorPt-1000","type":"WaterMeter","collection_time":30000,"publishing_time":60000},{"id":"ConductivitySensor","type":"WaterMeter","collection_time":30000,"publishing_time":60000},{"id":"DissolvedOxygenSensor","type":"WaterMeter","collection_time":30000,"publishing_time":60000},{"id":"pHSensor","type":"WaterMeter","collection_time":30000,"publishing_time":60000},{"id":"OxidationReductionPotentialSensor","type":"WaterMeter","collection_time":30000,"publishing_time":60000},{"id":"TurbiditySensor","type":"WaterMeter","collection_time":30000,"publishing_time":60000}]},{"id":"libeliumSmartWaterIons01","latitude":57.290411,"longitude":-8.074406,"sensors":[{"id":"NitriteNO2Sensor","type":"WaterMeter","collection_time":30000,"publishing_time":60000}]},{"id":"sc01","latitude":57.290411,"longitude":-8.074406,"sensors":[{"id":"temperatureSensor","type":"ambientSensors","collection_time":30000,"publishing_time":60000},{"id":"luminositySensor","type":"ambientSensors","collection_time":30000,"publishing_time":60000},{"id":"dustSensor","type":"ambientSensors","collection_time":30000,"publishing_time":60000},{"id":"humiditySensor","type":"ambientSensors","collection_time":30000,"publishing_time":60000},{"id":"soundSensor","type":"ambientSensors","collection_time":30000,"publishing_time":60000}]}]'


#print(DevicesConnected)
jsonDevicesConnected = json.loads(DevicesConnected)
#print(jsonDevicesConnected)

for devices in jsonDevicesConnected:
    if(devices['id']=='sc01' or devices['id']=='sc02' ):
        for sensors in devices['sensors']:
            print("dev."+devices['id']+"."+sensors['id'])
            ConsumerSensorTemperature = SensorKafkaConsumer(devices['id'], sensors['id'],"dev."+devices['id']+"."+sensors['id'], "Fog-IME", "18.218.147.104:9092")
            ConsumerSensorTemperature.start()
            