import config, replica, os, shutil, client
import time, multiprocessing
import random
import sys

# Configure command line options
DEFAULT_NUM_FAILURES = 2
DEFAULT_NUM_CLIENTS = 2
DEBUG = False
SKIP = True
MODE = 1
SERVER_PORTS_INFO = "server_ports.txt"
MASTER_PORT_INFO = "master_port.txt"

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])

if __name__=="__main__":
	print 'begin'
	if find_args('-num_failures') != None:
		DEFAULT_NUM_FAILURES = int(find_args('-num_failures'))
	if find_args('-mode') != None:
		MODE = int(find_args('-mode'))
	if find_args('-server_ports') != None:
		SERVER_PORTS_INFO = find_args('-server_ports')
	if find_args('-master_port') != None:
		MASTER_PORT_INFO = find_args('-master_port')
	if find_args('-replica_index') != None:
		i = int(find_args('-replica_index'))
	if find_args('-skip') != None:
		SKIP = int(find_args('-skip'))
		if SKIP == 1: SKIP = True
		else: SKIP = False
	random.seed(1)
	server_ports_info = read_ports_info(SERVER_PORTS_INFO , 2*DEFAULT_NUM_FAILURES+1)
	master_port_info = read_ports_info(MASTER_PORT_INFO , 1)
	p = multiprocessing.Process(target=replica.Replica, args = (DEFAULT_NUM_FAILURES , i , server_ports_info , master_port_info , DEBUG , SKIP)) #f, ID, port_info
	print "start replica %d" % (i)
	#for line in sys.stdin:
	#	print line
	raw_input("Press Enter to continue...")
	p.start()
