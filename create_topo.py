from mininet.node import  OVSKernelSwitch
from mininet.link import TCLink
from reg import utils_hosts
def create(net):
	#adicionando os switches
	QTD_SWITCHES=7
	s1=net.addSwitch('s1',cls=OVSKernelSwitch, failMode='standalone')
	s2=net.addSwitch('s2',cls=OVSKernelSwitch, failMode='standalone')
	s3=net.addSwitch('s3',cls=OVSKernelSwitch, failMode='standalone')
	s4=net.addSwitch('s4',cls=OVSKernelSwitch, failMode='standalone')
	s5=net.addSwitch('s5',cls=OVSKernelSwitch, failMode='standalone')
	s6=net.addSwitch('s6',cls=OVSKernelSwitch, failMode='standalone')
	s7=net.addSwitch('s7',cls=OVSKernelSwitch, failMode='standalone')

	#largura de banda entre os switches 50MB
	bw=50
	link_features_sw = {'bw':bw}
	
	#ligando os switches e criando uma topologia linear simples
	net.addLink('s1', 's2',cls=TCLink, **link_features_sw)
	net.addLink('s2', 's3',cls=TCLink, **link_features_sw)
	net.addLink('s3', 's4',cls=TCLink, **link_features_sw)
	net.addLink('s4', 's5',cls=TCLink, **link_features_sw)
	net.addLink('s5', 's6',cls=TCLink, **link_features_sw)
	net.addLink('s6', 's7',cls=TCLink, **link_features_sw)


	#criacao dos hosts baseado no arquivo data_host
	QTD_HOSTS=len(utils_hosts.return_hosts())
	for i in range(1,QTD_HOSTS+1):
		net.addHost('h%d'%i,ip='10.0.0.%d/24'%i)
	
	#largura de banda entre switches e hosts (devices e gateways) 3MB
	bw_hosts=3
	s=1
    #criacao simples dos links entre switches e hosts (devices e gateways)
	for i in range(1,QTD_HOSTS+1):
		link_features = {'bw':bw_hosts}
		net.addLink(net.get("s%d"%(s)),net.get("h%d"%i),cls=TCLink, **link_features)
		if(s==QTD_SWITCHES):
			s=0
		s=s+1


