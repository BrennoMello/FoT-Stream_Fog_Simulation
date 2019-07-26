#!/usr/bin/python

from mininet.cli import CLI
from mininet.net import Mininet
from mininet.link import TCLink
from mininet.log import lg
from mininet.node import Node
from mininet.topolib import TreeNet
from reg import utils_hosts
from time import sleep
import time
import argparse
import logging
from traceback import print_exc

############## Parse Arguments #####
parser = argparse.ArgumentParser(prog='virtual_device', usage='%(prog)s [options]', description='Virtual Device')
parser.add_argument('-p','--port', type=str, help='Broker Ports',required=True)
parser.add_argument('-d','--direc', type=str, help='Direct data set',required=True)
args = parser.parse_args()


#Path Server and Gateway
gateway_path='/home/openflow/service-mix/'
server_path='/home/openflow/service-mix/server'

#################################
def startNAT( root, inetIntf='eth0', subnet='10.0/8' ):
    """Start NAT/forwarding between Mininet and external network
    root: node to access iptables from
    inetIntf: interface for internet access
    subnet: Mininet subnet (default 10.0/8)="""

    # Identificar interface conectada a rede
    localIntf = root.defaultIntf()

    # Limpar regras ativas
    root.cmd( 'iptables -F' )
    root.cmd( 'iptables -t nat -F' )

    # Criar entradas de trfego padrao sem corresondencia
    root.cmd( 'iptables -P INPUT ACCEPT' )
    root.cmd( 'iptables -P OUTPUT ACCEPT' )
    root.cmd( 'iptables -P FORWARD DROP' )

    # Configure NAT
    root.cmd( 'iptables -I FORWARD -i', localIntf, '-d', subnet, '-j DROP' )
    root.cmd( 'iptables -A FORWARD -i', localIntf, '-s', subnet, '-j ACCEPT' )
    root.cmd( 'iptables -A FORWARD -i', inetIntf, '-d', subnet, '-j ACCEPT' )
    root.cmd( 'iptables -t nat -A POSTROUTING -o ', inetIntf, '-j MASQUERADE' )

    # Encaminhamento kernel
    root.cmd( 'sysctl net.ipv4.ip_forward=1' )

def stopNAT( root ):
    """Stop NAT/forwarding between Mininet and external network"""
    # Limpar regras ativas
    root.cmd( 'iptables -F' )
    root.cmd( 'iptables -t nat -F' )

    # Indicar o kernel p parar encaminhamento 
    root.cmd( 'sysctl net.ipv4.ip_forward=0' )

def fixNetworkManager( root, intf ):
    """Prevent network-manager from messing with our interface,
       by specifying manual configuration in /etc/network/interfaces
       root: a node in the root namespace (for running commands)
       intf: interface name"""
    cfile = '/etc/network/interfaces'
    line = '\niface %s inet manual\n' % intf
    config = open( cfile ).read()
    if line not in config:
        print '*** Adding', line.strip(), 'to', cfile
        with open( cfile, 'a' ) as f:
            f.write( line )
        # Probably need to restart network-manager to be safe -
        # hopefully this won't disconnect you
        root.cmd( 'service network-manager restart' )

def connectToInternet( network, switch='s1', rootip='10.254', subnet='10.0/8'):
    """Connect the network to the internet
       switch: switch to connect to root namespace
       rootip: address for interface in root namespace
       subnet: Mininet subnet"""
    switch = network.get( switch )
    prefixLen = subnet.split( '/' )[ 1 ]

    # Criando node no namespace raiz
    root = Node( 'root', inNamespace=False )

    # Interface gerenciador prevencao
    fixNetworkManager( root, 'root-eth0' )

    # Criando link
    link = network.addLink( root, switch )
    link.intf1.setIP( rootip, prefixLen )

    # Start network
    network.start()

    # Start NAT
    startNAT( root )

    # Estabelecendo rotas entre hosts
    for host in network.hosts:
        host.cmd( 'ip route flush root 0/0' )
        host.cmd( 'route add -net', subnet, 'dev', host.defaultIntf() )
        host.cmd( 'route add default gw', rootip )

    return root

def init_sensors(net):
	global args
	i=1
	
	#pega todos os devices/sensores
	d=utils_hosts.return_hosts_per_type('device')
	ass=utils_hosts.return_association()
	print ("Init Sensors")	
	
	#inciar mosquitto nos devices
	#for i in range(0,len(d)):
	#	net.get(d[i].name).cmd('mosquitto &')
	
	time.sleep(5)
	
	#iniciar devices virtuais
	for i in range(0,len(d)):
		print('python virtual_dev.py -n '+ass[i].name+' -s temperatureSensor -p '+args.port+' -i '+ass[i].gateway+' -d '+args.direc+ ' -m  ' + d[i].moteid +' > virtual-device &')
		term = net.get(d[i].name)
		#term.cmd('screen -S virtual-dev')
		#term.cmd('screen -r virtual-dev')
		term.cmd('cd /home/openflow/FoT-Simulation; python virtual_dev.py -n '+ass[i].name+' -s temperatureSensor -p '+args.port+' -i '+ass[i].gateway+' -d '+args.direc+ ' -m  ' + d[i].moteid +' > virtual-device &')

	time.sleep(7)

def init_server(net):
	print("Init Server")
	g=utils_hosts.return_hosts_per_type('server')
	
	for i in range(0,len(g)):
		#iniciar kafka e ....
		print(g[i].name)
		server = net.get(g[i].name)
		server.cmd('cd /home/openflow/FoT-Simulation/kafka_2.11-1.0.0; bin/zookeeper-server-start.sh config/zookeeper.properties > zookeeper-log &')
		time.sleep(10)
		server.cmd('cd /home/openflow/FoT-Simulation/kafka_2.11-1.0.0; bin/kafka-server-start.sh config/server.properties > kafka-log &')
		print('python FoT-StreamServer.py -n '+ g[i].name + ' -i ' + g[i].ip  + ' -p 9092 > server-log-'+ g[i].name +' &')
		server.cmd('cd /home/openflow/FoT-Simulation/FoTStreamServer/kafka-mqtt; python FoT-StreamServer.py -n '+ g[i].name + ' -i '+ g[i].ip +' -p 9092 > server-log-'+ g[i].name +' &')	
		
		sleep(5)
		

def init_gateways(net):
	print("Init Gateways")
	g=utils_hosts.return_hosts_per_type('gateway')
	ass=utils_hosts.return_association_server()
	
	for i in range(0,len(g)):
		#iniciar mosquitto se precisar, comentado por padrao
		gateway = net.get(g[i].name)
		gateway.cmd('mosquitto &')
		print('python FoT-StreamGateway.py -n '+ g[i].name + ' -i ' + ass[i].server +' -p 9092 > gateway-log-'+g[i].name+ ' &')
		gateway.cmd('cd /home/openflow/FoT-Simulation; python FoT-StreamGateway.py -n '+ g[i].name + ' -i ' + ass[i].server +' -p 9092 > gateway-log-'+g[i].name+' &')	
		
		sleep(5)

	
	#for i in range(0,len(g)):
		#iniciar mosquitto se precisar, comentado por padrao
		#net.get(g[i].name).cmd('mosquitto &')
		#if((i+1)<10):
			#net.get(g[i].name).cmd('cd '+gateway_path+'/0'+str(i+1)+'/apache-servicemix-7.0.1/bin; ./servicemix &')
		#else:
			#net.get(g[i].name).cmd('cd '+gateway_path+'/'+str(i+1)+'/apache-servicemix-7.0.1/bin; ./start')
		#sleep(5)

def stop_gateways(net):
	g=utils_hosts.return_hosts_per_type('gateway')
	
	#for i in range(0,len(g)):
		#iniciar mosquitto se precisar, comentado por padrao
		#net.get(g[i].name).cmd('mosquitto &')
		#if((i+1)<10):
			#net.get(g[i].name).cmd('cd '+gateway_path+'/0'+str(i+1)+'/apache-servicemix-7.0.1/bin; ./stop &')
		#else:
			#net.get(g[i].name).cmd('cd '+gateway_path+'/'+str(i+1)+'/apache-servicemix-7.0.1/bin; ./stop &')
		#sleep(5)

def stop_servers(net):
	print("Stop Servers")
	g=utils_hosts.return_hosts_per_type('server')
	
	for i in range(0,len(g)):
		#iniciar kafka e ....
		print(g[i].name)
		server = net.get(g[i].name)
		server.cmd('cd /home/openflow/FoT-Simulation/kafka_2.11-1.0.0; bin/kafka-server-stop.sh > kafka-log &')
		time.sleep(20)
		server.cmd('cd /home/openflow/FoT-Simulation/kafka_2.11-1.0.0; zookeeper-server-stop.sh > zookeeper-log &')
		print('python FoT-StreamServer.py -n '+ g[i].name + ' -i ' + g[i].ip  + ' -p 9092 > server &')
				
		sleep(5)

def init_flow(net):
	print ("Temp: Init Flow")
	g=utils_hosts.return_hosts_per_type('gateway')
	ass=utils_hosts.return_association()
	for i in range(0,len(g)):
		for j in range(0,len(ass)):
			if(g[i].name==ass[j].name_gateway):
				print(g[i].name)
				print("mosquitto_pub -t 'dev/"+ass[j].name+"' -m 'FLOW INFO temperatureSensor {collect:1000,publish:1000}'")
				net.get(g[i].name).cmd("mosquitto_pub -t 'dev/"+ass[j].name+"' -m 'FLOW INFO temperatureSensor {collect:1000,publish:1000}'")
				time.sleep(0.2)



if __name__ == '__main__':
	lg.setLogLevel('info')
	logging.basicConfig(filename = 'app.log', level = logging.INFO)
	
	try:
		net = Mininet(link=TCLink)
		
		#criar switches, hosts e topologia
		import create_topo
		create_topo.create(net)
		
		# Configurar e iniciar comunicacao externa
		rootnode = connectToInternet( net )
		
		init_server(net)
		init_gateways(net)
		init_sensors(net)
		
		#init flow eh temporario
		init_flow(net)
		
		CLI( net )
		# Shut down NAT
		stopNAT( rootnode )
		stop_gateways(net)
		stop_servers(net)
		time.sleep(3)
		net.stop()
	except Exception as inst:
		print_exc()
		print(inst)
		logging.exception(str(inst))
