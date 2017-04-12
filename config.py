class Message():
    mtype = None
    request_id = None
    client_id = None
    client_request_id = None
    sender_id = None
    value = None
    received_propose_list = None
    IAmYourLeader = 0 # Sender_id
    YouAreMyLeader = 1 # previous Sender_id, value
    ProposeValue = 2 # Sender_id value
    AcceptValue = 3 # value
    TimeOut = 4
    Request = 5 # value
    Logged = 6
    def __init__(self, mtype = None, request_id = None, client_id = None, \
    client_request_id = None, sender_id = None, value = None, received_propose_list = None):
        self.mtype = mtype
        self.request_id = request_id
        self.client_request_id = client_request_id
        self.client_id = client_id
        self.sender_id = sender_id
        self.value = value
        self.received_propose_list = received_propose_list
