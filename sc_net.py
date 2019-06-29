import commands
import re
import os
import timeit
from reg import utils_hosts
import time
import argparse


TIME_SIMULATION=1200
ini=timeit.default_timer()
while True:
	time.sleep(1)
	fim=timeit.default_timer()
	
	net= commands.getoutput("tc -s qdisc show")
	a = re.search(r'\b(Sent)\b', str(net))
	b = re.search(r'\b(bytes)\b', str(net))
	network = str(net[int(a.end()+1):int(b.start()-1)])
	network=str(float(network)/1000)
	print network+" KBytes"
	time.sleep(3)
	if((fim-ini)>TIME_SIMULATION):
		break
