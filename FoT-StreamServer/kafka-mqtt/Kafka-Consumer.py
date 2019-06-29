from kafka import KafkaConsumer
import json

class SensorKafkaConsumer:
    kafka_consumer = None
    topic = ""
    group = ""
    def __init__ (self , topic, group, broker_server):
        self.topic = topic
        self.group = group
        self.kafka_consumer = KafkaConsumer(topic,
                         group_id=group,
                         bootstrap_servers=[broker_server])
        loopKafka(self)

    def loopKafka(self):
        consumer.subscribe([topic])

        # Now loop on the consumer to read messages
        running = True
        while running:
            message = kafka_consumer.poll()
            application_message = json.load(message.value.decode())
            print(application_message)
   
#JSON with array of devices
DevicesConnected = {"id":"libeliumSmartWater01","latitude":53.290411,"longitude":-9.074406,
"sensors":[{"id":"TemperatureSensorPt-1000","type":"WaterMeter","collection_time":30000,"publishing_time":60000},
{"id":"ConductivitySensor","type":"WaterMeter","collection_time":30000,"publishing_time":60000},
{"id":"DissolvedOxygenSensor","type":"WaterMeter","collection_time":30000,"publishing_time":60000},
{"id":"pHSensor","type":"WaterMeter","collection_time":30000,"publishing_time":60000},
{"id":"OxidationReductionPotentialSensor","type":"WaterMeter","collection_time":30000,
"publishing_time":60000},{"id":"TurbiditySensor","type":"WaterMeter","collection_time":30000,
"publishing_time":60000}]},{"id":"libeliumSmartWaterIons01","latitude":57.290411,"longitude":-8.074406,
"sensors":[{"id":"NitriteNO2Sensor","type":"WaterMeter","collection_time":30000,"publishing_time":60000}]},
{"id":"sc01","latitude":57.290411,"longitude":-8.074406,"sensors":[{"id":"temperatureSensor","type":"ambientSensors",
"collection_time":30000,"publishing_time":60000},{"id":"luminositySensor","type":"ambientSensors","collection_time":30000,
"publishing_time":60000},{"id":"dustSensor","type":"ambientSensors","collection_time":30000,"publishing_time":60000},
{"id":"humiditySensor","type":"ambientSensors","collection_time":30000,"publishing_time":60000},{"id":"soundSensor",
"type":"ambientSensors","collection_time":30000,"publishing_time":60000}]}

#parsedJson = json.dumps(DevicesConnected)
#print(parsedJson)

# To consume latest messages and auto-commit offsets
consumer = KafkaConsumer('my-topic',
                         group_id='my-group',
                         bootstrap_servers=['localhost:9092'])
for message in consumer:
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value))

# consume earliest available messages, don't commit offsets
KafkaConsumer(auto_offset_reset='earliest', enable_auto_commit=False)

# consume json messages
KafkaConsumer(value_deserializer=lambda m: json.loads(m.decode('ascii')))

# consume msgpack
KafkaConsumer(value_deserializer=msgpack.unpackb)

# StopIteration if no message after 1sec
KafkaConsumer(consumer_timeout_ms=1000)

# Subscribe to a regex topic pattern
consumer = KafkaConsumer()
consumer.subscribe(pattern='^awesome.*')

# Use multiple consumers in parallel w/ 0.9 kafka brokers
# typically you would run each on a different server / process / CPU
consumer1 = KafkaConsumer('my-topic',
                          group_id='my-group',
                          bootstrap_servers='my.server.com')
consumer2 = KafkaConsumer('my-topic',
                          group_id='my-group',
                          bootstrap_servers='my.server.com')