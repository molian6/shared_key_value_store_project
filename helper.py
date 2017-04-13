import socket
import ruamel.yaml as yaml
import json
import time
import random
from config import Message
import math

max_ = math.pow(2 , 64)

def send_message(host, port_number, m):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect((host, port_number))
    s.sendall(m)
    s.close()

def create_listen_sockets(host, port_number):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind((host, port_number))
    return s

def encode_message(m):
    message = {
        "mtype": m.mtype,
        "request_id": m.request_id,
        "client_id": m.client_id,
        "client_request_id": m.client_request_id,
        "sender_id": m.sender_id,
        "value": m.value,
        "received_propose_list": m.received_propose_list
        "command": m.command,
        "key": m.key
    }
    return json.dumps(message)

def decode_message(msg):
    msg_dict = yaml.safe_load(msg)
    m = Message(msg_dict["command"], msg_dict["key"], msg_dict["mtype"], msg_dict["request_id"], msg_dict["client_id"], msg_dict["client_request_id"], msg_dict["sender_id"], msg_dict["value"], msg_dict["received_propose_list"])
    return m


def distance(a , b):
    if b <= a:
        dis = a - b
    else:
        dis = max_ + a - b
    return dis

def consistent_hashing(value):

    return idx #integer
