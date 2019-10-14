import pandas as pd
import time
import logging


class DataSetReader(object):


    def __init__(self, direc, sensorName, moteId):
		self.direc = direc
		self.SensorName = sensorName
		self.MoteId = moteId 
		self.open_data = None
		self.lineTemp = 0
		self.lineHumid = 0
		self.lineLigh = 0
		self.lineVolt = 0
		self.countLine = 0
		self.value = 0
		logging.basicConfig(filename = 'app.log', level = logging.INFO)
		self.reader_data()
	
	#chunksize=100
	#iterator=True
    def reader_data(self):
        #self.open_data = open(self.direc, 'rb')
		print(self.direc + self.SensorName + '-' + self.MoteId + '.csv')
        #self.count_row = pd.read_csv(self.direc + self.SensorName + '-' + self.MoteId + '.csv', sep=",", header=0).shape[0]
		f = open(self.direc + self.SensorName + '-' + self.MoteId + '.csv','r')
		self.count_row = len(f.readlines())
		f.close()
		
		self.open_data = pd.read_csv(self.direc + self.SensorName + '-' + self.MoteId + '.csv', sep=",", header=0, chunksize=10)
        #self.countLine = self.open_data.shape
		print("Data Readear: open data sets")

    def next_value(self, sensor):    
		
        #line = self.open_data.readline()
        #splitLine = line.split(" ")
        #print("Data reader: next value")
        
        #print("Qtd lines " + str(self.count_row))
        if(sensor=="temperatureSensor"):
            try:
				print self.lineTemp
				print self.count_row-3
				if(self.lineTemp==self.count_row-3):
				#if(self.lineTemp==50):
					#self.reader_data()
					self.lineTemp = 0
					del self.open_data 
					self.open_data = pd.read_csv(self.direc + self.SensorName + '-' + self.MoteId + '.csv', sep=",", header=0, chunksize=10)
					print('first line')
				
				self.value = pd.DataFrame(self.open_data.get_chunk(1))
				self.value = self.value.get_values()
				self.value = self.value[0][4]
				
				self.lineTemp = self.lineTemp+1
				#self.countLine = self.countLine+1
				#logging.exception("line "+ str(self.countLine))
            except Exception as inst:
				print(inst)
				logging.exception(str(inst)) 
        
        if(sensor=="humiditySensor"):
            if(self.lineHumid==2313155):
				self.lineHumid = 0    
				
				value = pd.DataFrame(self.open_data.get_chunk(1))
				value = value.get_values()
				value = value[0][4]
				
				self.lineHumid = self.lineHumid+1
        if(sensor=="lighSensor"):
            if(self.lineLigh==self.countLine[0]-1):
                self.lineLigh = 0 
            value = self.open_data.loc[self.lineLigh] 
            value = value[6]
            self.lineLigh = self.lineLigh+1 
        if(sensor=="voltageSensor"):
            if(self.lineVolt==self.countLine[0]-1):
                self.lineVolt = 0 
            value = self.open_data.loc[self.lineVolt] 
            value = value[7]
            self.lineVolt = self.lineVolt+1 

        #print(value)
        return self.value

#dataSet = DataSetReader("/home/openflow/FoT-Simulation/dataset/dataSet_temp.txt")

#thread = True
#while thread == True:
	#dataSet.next_value("temperatureSensor")
    #time.sleep(5)
