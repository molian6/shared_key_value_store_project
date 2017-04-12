import config, replica, os, shutil, client
import time, multiprocessing
import random
import sys

# Configure command line options
DEFAULT_NUM_FAILURES = 2
DEFAULT_NUM_CLIENTS = 2
DEBUG = False
SKIP = False
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
	if find_args('-client_index') != None:
		i = int(find_args('-client_index'))
	random.seed(1)
	server_ports_info = read_ports_info(SERVER_PORTS_INFO , 2*DEFAULT_NUM_FAILURES+1)
	client_ports_info = read_ports_info(CLIENT_PORTS_INFO , DEFAULT_NUM_CLIENTS)
	e = multiprocessing.Event()
	p = multiprocessing.Process(target = client.Client , args = (client_ports_info[i][0] , client_ports_info[i][1], i , server_ports_info , e))
	raw_input("Press Enter to continue...")
	p.start()
	while True:
		raw_input("Press to send message")
		if e.is_set() == False:
			e.set()
			'Client %d sends a message.' % (i)
		else:
			print 'Client %d is waiting for responce of the previous message.'
