import pandas as pd
import time

class DataSetReader(object):

    
    def __init__(self, dir):
        self.dir = dir
        self.open_data = None
        self.lineTemp = 0
        self.lineHumid = 0
        self.lineLigh = 0
        self.lineVolt = 0
        self.reader_data()

    def reader_data(self):
        #self.open_data = open(self.dir, 'rb')
        self.open_data = pd.read_csv(self.dir, sep=" ", header=None)
        self.countLine = self.open_data.shape

    def next_value(self, sensor):    
        #line = self.open_data.readline()
        #splitLine = line.split(" ")
        if(sensor=="temperature"):
            if(self.lineTemp==self.countLine):
                self.lineTemp = 0    
            value = self.open_data.loc[self.lineTemp] 
            value = value[4]
            self.lineTemp = self.lineTemp+1
        if(sensor=="humidity"):
            if(self.lineHumid==self.countLine):
                self.lineHumid = 0    
            value = self.open_data.loc[self.lineHumid] 
            value = value[5]
            self.lineHumid = self.lineHumid+1            
        if(sensor=="ligh"):
            if(self.lineLigh==self.countLine):
                self.lineLigh = 0 
            value = self.open_data.loc[self.lineLigh] 
            value = value[6]
            self.lineLigh = self.lineLigh+1 
        if(sensor=="voltage"):
            if(self.lineVolt==self.countLine):
                self.lineVolt = 0 
            value = self.open_data.loc[self.lineVolt] 
            value = value[7]
            self.lineVolt = self.lineVolt+1 

        print(value)
        return value

#dataSet = DataSetReader("/media/brenno/Brenno/DataSet-intel/dataset-intel/data.txt")

#thread = True
#while thread == True:
#    dataSet.next_value("temperature")
#    time.sleep(5)