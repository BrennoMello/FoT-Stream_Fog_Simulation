import commands
import re
import os
import timeit
from reg import utils_hosts
import time
import argparse

parser = argparse.ArgumentParser(prog='sc_net', usage='%(prog)s [options]', description='sc_net')
parser.add_argument('-n','--name', type=str, help='Host Name',required=True)
args = parser.parse_args()

name = args.name

TIME_SIMULATION=1200
ini=timeit.default_timer()
f=open('network-'+name,'w+')
while True:
	time.sleep(1)
	fim=timeit.default_timer()
	
	net= commands.getoutput("tc -s qdisc show")
	a = re.search(r'\b(Sent)\b', str(net))
	b = re.search(r'\b(bytes)\b', str(net))
	network = str(net[int(a.end()+1):int(b.start()-1)])
	network=str(float(network)/1000)
	print network+" KBytes"
	f.write(network+" KBytes")
	f.write("\n") 
	time.sleep(3)
	#if((fim-ini)>TIME_SIMULATION):
	#	break
