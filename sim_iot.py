#!/usr/bin/python

from mininet.cli import CLI
from mininet.log import lg
from mininet.node import Node
from mininet.topolib import TreeNet
import time
import argparse

############## Parse Arguments
parser = argparse.ArgumentParser(prog='virtual_device', usage='%(prog)s [options]', description='Virtual Device')
parser.add_argument('-p','--port', type=str, help='Broker Ports',required=True)
args = parser.parse_args()


#Path Server and Gateway
gateway_path='/home/openflow/service-mix/gateway'
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

    # Estabelecndo rotas entre hosts
    for host in network.hosts:
        host.cmd( 'ip route flush root 0/0' )
        host.cmd( 'route add -net', subnet, 'dev', host.defaultIntf() )
        host.cmd( 'route add default gw', rootip )

    return root

def init_sensors(net):

	global args
	i=1	
	for host in net.hosts:
		#net.get(host.name).cmd("sudo python virtual_dev.py -n sensor"+str(i)+" -g gateway01 -p "+args.port+" -i "+args.ip+" -t "+args.type+" -m "+args.model+" -c "+args.cycle+" &")
		net.get(host.name).cmd("sudo python virtual_dev.py -n sensor"+str(i)+" -g gateway01 -p "+" &")
		i=i+1
		time.sleep(1)



def init_gateways(net):
	g=utils_hosts.return_hosts_per_type('gateway')
	for i in range(0,len(g)):
		#iniciar mosquitto se precisar, comentado por padrao
		#net.get(g[i].name).cmd('mosquitto &')
		if((i+1)<10):
			net.get(g[i].name).cmd('cd '+gateway_path+'/0'+str(i+1)+'/apache-servicemix-6.1.0/bin; ./start')
		else:
			net.get(g[i].name).cmd('cd '+gateway_path+'/'+str(i+1)+'/apache-servicemix-6.1.0/bin; ./start')
		sleep(5)

if __name__ == '__main__':
    lg.setLogLevel( 'info')
	net = Mininet(link=TCLink)
    
    #criar switches, hosts e topologia
	import create_topo
	create_topo.create(net)
    
    # Configurar e iniciar comunicacao externa
    rootnode = connectToInternet( net )
    
    #init_sensors(net)
    
    CLI( net )
    # Shut down NAT
    stopNAT( rootnode )
    net.stop()
