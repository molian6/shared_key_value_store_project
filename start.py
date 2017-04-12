# start the paxo service
# Start the entire paxo service, start 2f+1 replicas
# Manage meta data for replicas


import config, replica, os, shutil, client
import time, multiprocessing
import random
import sys

# Configure command line options
DEFAULT_NUM_FAILURES = 1
DEFAULT_NUM_CLIENTS = 1
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

def Paxoservice():
	random.seed(1)
	server_ports_info = read_ports_info(SERVER_PORTS_INFO , 2*DEFAULT_NUM_FAILURES+1)
	client_ports_info = read_ports_info(CLIENT_PORTS_INFO , DEFAULT_NUM_CLIENTS)

	# create replicas
	client_list = []
	for i in range(DEFAULT_NUM_CLIENTS):
		e = multiprocessing.Event()
		p = multiprocessing.Process(target = client.Client , args = (client_ports_info[i][0] , client_ports_info[i][1], i , server_ports_info , e))
		p.start()
		client_list.append([p , e])
	print 'Create %d clients successfully!' % (DEFAULT_NUM_CLIENTS)
	replica_list = []
	for i in range(2*DEFAULT_NUM_FAILURES+1):
		p = multiprocessing.Process(target=replica.Replica, args = (DEFAULT_NUM_FAILURES , i , server_ports_info , client_ports_info , DEBUG , SKIP)) #f, ID, port_info
		p.start()
		replica_list.append(p)
	print 'Create %d replicas successfully!' % (2*DEFAULT_NUM_FAILURES+1)

	# operation start from here

	time.sleep(2)

	if MODE == 1:
		t = time.time() + 10
		print 'Start sending messages at %s.' % (time.ctime(int(time.time())))
		while time.time() < t:
			a = random.randint(0,DEFAULT_NUM_CLIENTS-1)
			if client_list[a][1].is_set() == False:
				client_list[a][1].set()
		print 'Stop sending messages at %s.' % (time.ctime(int(time.time())))
		time.sleep(5)

	if MODE == 2:
		t = time.time() + 5
		print 'Start sending messages at %s.' % (time.ctime(int(time.time())))
		while time.time() < t:
			a = random.randint(0,DEFAULT_NUM_CLIENTS-1)
			if client_list[a][1].is_set() == False:
				client_list[a][1].set()

		replica_list[0].terminate()
		print 'Replica 0 fails at %s.' % (time.ctime(int(time.time())))
		t = time.time() + 10
		while time.time() < t:
			a = random.randint(0,DEFAULT_NUM_CLIENTS-1)
			if client_list[a][1].is_set() == False:
				client_list[a][1].set()
		print 'Stop sending messages at %s.' % (time.ctime(int(time.time())))
		time.sleep(5)

	if MODE == 3:
		num_request = 0
		print 'Start sending messages at %s.' % (time.ctime(int(time.time())))
		while num_request < 10:
				a = random.randint(0,DEFAULT_NUM_CLIENTS-1)
				if client_list[a][1].is_set() == False:
					client_list[a][1].set()
					num_request += 1

		for i in range(DEFAULT_NUM_FAILURES):
			replica_list[i].terminate()
			print 'Replica %d fails at %s.' % (i , time.ctime(int(time.time())))
			num_request = 0
			while num_request < 10:
				a = random.randint(0,DEFAULT_NUM_CLIENTS-1)
				if client_list[a][1].is_set() == False:
					client_list[a][1].set()
					num_request += 1
		print 'Stop sending messages at %s.' % (time.ctime(int(time.time())))
		time.sleep(10)

	if MODE == 4:
		num_request = 0
		print 'Start sending messages at %s.' % (time.ctime(int(time.time())))
		while num_request < 40:
				a = random.randint(0,DEFAULT_NUM_CLIENTS-1)
				if client_list[a][1].is_set() == False:
					client_list[a][1].set()
					num_request += 1
		print 'Stop sending messages at %s.' % (time.ctime(int(time.time())))
		time.sleep(15)

	checklist = []
	# terminate
	for p in client_list:
		if p[0].is_alive():
			p[0].terminate()
		p[0].join()
	for i,p in enumerate(replica_list):
		if p.is_alive():
			p.terminate()
			checklist.append(i)
		p.join()

	print '\n'
	print 'Start check:'
	for k,i in enumerate(checklist[:-1]):
		for j in checklist[k+1:]:
			print 'diff log%d.txt log%d.txt' % (i,j)
			os.system('diff log%d.txt log%d.txt' % (i,j))
			print '\n'

def find_args(cmd):
	for i,x in enumerate(sys.argv):
		if x == cmd:
			return sys.argv[i+1]
	return None

if __name__ == "__main__":
	print 'Please type your password to validate longer message in UDP protocal.'
	os.system('sudo sysctl -w net.inet.udp.maxdgram=65535')

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

	print 'In this experimemt, we will create %d clients and %d replicas.' % (DEFAULT_NUM_CLIENTS , DEFAULT_NUM_FAILURES*2+1) 
	if MODE == 1:
		print 'Normal operation: the clients work in batch mode, where they send messages continously in %s seconds.' % (10)
	elif MODE == 2:
		print 'The first primary fails after 5 seconds. The system will run with the new primary for 5 seconds.'
	elif MODE == 3:
		print 'System runs %d periods. In each turn, clients will send 10 messages, then terminate the primary, and repeat until the first %d primaries fail.' % (DEFAULT_NUM_FAILURES , DEFAULT_NUM_FAILURES)
	elif MODE == 4:
		print 'System will skip the 9-th message in every 10 messages. Send 40 messages in total.'
		SKIP = True

	Paxoservice()
