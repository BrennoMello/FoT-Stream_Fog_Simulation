import paho.mqtt.client as mqtt
import json, time
from threading import Thread

class DeviceMqttClient(Thread):
    clientMqtt = None
    broker_address = ""
    deviceId = ""
    jsonSensorsConnected = {}
    def __init__ (self , deviceId, broker_address, jsonSensorsConnected):
        Thread.__init__(self)
        self.deviceId = deviceId
        self.broker_address = broker_address
        self.jsonSensorsConnected = jsonSensorsConnected
        self.clientMqtt = mqtt.Client("PublishGetMessage"+self.deviceId) #create new instance
        self.clientMqtt.connect(broker_address) #connect to broker
        print(self.deviceId)
       

    def run(self):
        running = True
        while running:
            
            for sensors in self.jsonSensorsConnected:
                #if(sensors['id']=='luminositySensor'):    
                    print("Topic " + "dev/" + self.deviceId)
                    print("Message " + "GET INFO " + sensors['id'])
                    self.clientMqtt.publish("dev/"+self.deviceId,"GET INFO " + sensors['id']) 
                    time.sleep(2)

            
broker_address="192.168.13.7" 

DevicesConnected = '[{"id":"sc02","latitude":57.290411,"longitude":-8.074406,"sensors":[{"id":"temperatureSensor","type":"ambientSensors","collection_time":30000,"publishing_time":60000},{"id":"luminositySensor","type":"ambientSensors","collection_time":30000,"publishing_time":60000},{"id":"dustSensor","type":"ambientSensors","collection_time":30000,"publishing_time":60000},{"id":"humiditySensor","type":"ambientSensors","collection_time":30000,"publishing_time":60000},{"id":"soundSensor","type":"ambientSensors","collection_time":30000,"publishing_time":60000}]},{"id":"libeliumSmartWater01","latitude":53.290411,"longitude":-9.074406,"sensors":[{"id":"TemperatureSensorPt-1000","type":"WaterMeter","collection_time":30000,"publishing_time":60000},{"id":"ConductivitySensor","type":"WaterMeter","collection_time":30000,"publishing_time":60000},{"id":"DissolvedOxygenSensor","type":"WaterMeter","collection_time":30000,"publishing_time":60000},{"id":"pHSensor","type":"WaterMeter","collection_time":30000,"publishing_time":60000},{"id":"OxidationReductionPotentialSensor","type":"WaterMeter","collection_time":30000,"publishing_time":60000},{"id":"TurbiditySensor","type":"WaterMeter","collection_time":30000,"publishing_time":60000}]},{"id":"libeliumSmartWaterIons01","latitude":57.290411,"longitude":-8.074406,"sensors":[{"id":"NitriteNO2Sensor","type":"WaterMeter","collection_time":30000,"publishing_time":60000}]},{"id":"sc01","latitude":57.290411,"longitude":-8.074406,"sensors":[{"id":"temperatureSensor","type":"ambientSensors","collection_time":30000,"publishing_time":60000},{"id":"luminositySensor","type":"ambientSensors","collection_time":30000,"publishing_time":60000},{"id":"dustSensor","type":"ambientSensors","collection_time":30000,"publishing_time":60000},{"id":"humiditySensor","type":"ambientSensors","collection_time":30000,"publishing_time":60000},{"id":"soundSensor","type":"ambientSensors","collection_time":30000,"publishing_time":60000}]}]'

#print(DevicesConnected)
jsonDevicesConnected = json.loads(DevicesConnected)
#print(jsonDevicesConnected)

jsonSensorsConnected = None

for devices in jsonDevicesConnected:
    #if(devices['id']=='sc01' or devices['id']=='sc02'):
    if(devices['id']=='sc02'):
        ##for sensors in devices['sensors']:
        print("Device " + devices['id'])
        deviceMqttClient = DeviceMqttClient(devices['id'], broker_address, devices['sensors'])
        deviceMqttClient.start()