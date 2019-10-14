import json
import fileinput
from operator import itemgetter

class to_object(object):
	def __init__(self, j):
		self.__dict__ = json.loads(j)
			

def return_hosts():
	f=open('/home/mininet/FoT-Simulation/reg/data_hosts.json','r')
	lines=len(f.readlines())
	f.close()
	f=open('/home/mininet/FoT-Simulation/reg/data_hosts.json','r')
	st2=[]
	st2=f.readlines()
	f.close()
	hosts=[]
	for i in range(0,(lines)):
		hosts.append(to_object(st2[i]))
	return hosts

		
def return_hosts_per_type(type_host):
	hosts=return_hosts()
	re = []
	for i in range(0,len(hosts)):
		if (hosts[i].type==type_host) :
			re.append(hosts[i])
	return re

	
def write_host(st):
	x=open('/home/minet/FoT-Simulation/reg/data_hosts.json','a')
	x.write(st+"\n")
	x.close()


def write_hosts(h):
	for i in range(0,len(h)):
		write_host(json.dumps(h[i]))


def return_host_per_name(name_host):
	h=return_hosts()
	for i in range(0,len(h)):
		if(str(h[i].name)==name_host or str(h[i].name_iot)==name_host):
			return h[i]
			
def return_association_server():
	lines = open('/home/mininet/FoT-Simulation/association_gateway_server.json','r')
	devices=[]
	for l in lines:
		#print(l)
		devices.append(to_object(l))
	lines.close()
	return devices	
		
def return_association():
	f=open('/home/mininet/FoT-Simulation/reg/association_hosts.json','r')
	lines=len(f.readlines())
	f.close()
	f=open('/home/mininet/FoT-Simulation/reg/association_hosts.json','r')
	st2=[]
	st2=f.readlines()
	f.close()
	devices=[]
	for i in range(0,(lines)):
		if(to_object(st2[i]).name_gateway!='cloud'):
			devices.append(to_object(st2[i]))
	return devices

