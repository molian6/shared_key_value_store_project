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
CLIENT_PORTS_INFO = "client_ports.txt"
CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])

def read_ports_info(filename, n):
	s = {}
	with open(filename) as f:
		for i in range(n):
			l = f.readline()
			info = l.split(' ')
			s[int(info[0])] = [info[1], int(info[2])]
	return s

def find_args(cmd):
	for i,x in enumerate(sys.argv):
		if x == cmd:
			return sys.argv[i+1]
	return None

if __name__=="__main__":
	print 'begin'
	if find_args('-num_failures') != None:
		DEFAULT_NUM_FAILURES = int(find_args('-num_failures'))
	if find_args('-mode') != None:
		MODE = int(find_args('-mode'))
	if find_args('-num_clients') != None:
		DEFAULT_NUM_CLIENTS = int(find_args('-num_clients'))
	if find_args('-server_ports') != None:
		SERVER_PORTS_INFO = find_args('-server_ports')
	if find_args('-client_ports') != None:
		CLIENT_PORTS_INFO = find_args('-client_ports')
	if find_args('-replica_index') != None:
		i = int(find_args('-replica_index'))
	if find_args('-skip') != None:
		SKIP = int(find_args('-skip'))
		if SKIP == 1: SKIP = True
		else: SKIP = False 
	random.seed(1)
	server_ports_info = read_ports_info(SERVER_PORTS_INFO , 2*DEFAULT_NUM_FAILURES+1)
	client_ports_info = read_ports_info(CLIENT_PORTS_INFO , DEFAULT_NUM_CLIENTS)
	p = multiprocessing.Process(target=replica.Replica, args = (DEFAULT_NUM_FAILURES , i , server_ports_info , client_ports_info , DEBUG , SKIP)) #f, ID, port_info
	print "start replica %d" % (i)
	#for line in sys.stdin:
	#	print line	
	raw_input("Press Enter to continue...")
	p.start()
		
