import json
import socket
import time
from threading import Thread
import random

base_port = 5000
node_ports = {}
node_communication_delays = {}
nodes = []


def get_node_address_and_port(node_id):
    global node_ports
    return 'localhost', node_ports[node_id]


def get_node_communication_delay(source_node_id, destination_node_id):
    global node_communication_delays
    return node_communication_delays[source_node_id][destination_node_id]

def string_number_plus_1(string_number):
    return str(int(string_number)+1)


def listen(node):
    node.listen()

def start(node):
    node.start()

class Node:
    def __init__(self, *args, **kwargs):
        global node_ports, base_port
        self.id = kwargs['id']
        self.status = "only-responder"
        self.submission_delay = kwargs['submission_delay']
        self.voting_delay_p1 = kwargs['voting_delay_p1']
        self.voting_delay_p2 = kwargs['voting_delay_p2']
        self.queue_size = 20
        self.received_proposals = []
        self.received_potential_leader_acks = []
        self.received_propose_acks = []
        self.voting_count_p2 = 0
        self.agreed_value = -1

        node_ports[self.id] = base_port + int(self.id)

        self._init_server()

    def _init_server(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        address, port = get_node_address_and_port(self.id)
        s.bind((address, port))
        # s.listen(self.queue_size)
        self.server = s

    def _send_with_delay(self, kwargs, destination_node_id):
        byte_array = json.dumps(kwargs).encode('utf-8')
        connection = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        connection.bind(('localhost', 0))
        destination_address, destination_port = get_node_address_and_port(destination_node_id)
        delay = get_node_communication_delay(self.id, destination_node_id)
        connection.connect((destination_address, destination_port))

        time.sleep(delay)

        # print(f"S source: {self.id} dest: {destination_node_id} port: {destination_port}")
        connection.send(byte_array)
        connection.close()

    def _get_max_received_leader_id_proposal(self):
        if len(self.received_proposals) == 0:
            return None
        max_received_leader_id_proposal = self.received_proposals[0]
        for received_proposal in self.received_proposals:
            if received_proposal['leader_id'] > max_received_leader_id_proposal['leader_id']:
                max_received_leader_id_proposal = received_proposal
        return max_received_leader_id_proposal
    
    def _get_max_received_potential_leader_acks_leader_id(self):
        if len(self.received_potential_leader_acks) == 0:
            return None
        max_received_potential_leader_acks_leader_id = self.received_potential_leader_acks[0]
        for received_potential_leader_ack in self.received_potential_leader_acks:
            if received_potential_leader_ack > max_received_potential_leader_acks_leader_id:
                max_received_potential_leader_acks_leader_id = received_potential_leader_ack
        return max_received_potential_leader_acks_leader_id
    
    def _broadcast(self, kwargs):
        global nodes
        for node in nodes:
            if node.id != self.id:
                t = Thread(target=self._send_with_delay, args=(kwargs, node.id,))
                t.start()

    def listen(self):
        while True and self.agreed_value == -1:
            byte_array = self.server.recv(4096)
            kwargs = json.loads(byte_array, encoding='utf-8')
            print(f"node {self.id}: {kwargs['nid']} {kwargs['type']} {kwargs['value']}")

            if self.status == 'only-responder':
                if kwargs['type'] == 'POTENTIAL_LEADER':
                    leader_id = kwargs['value']
                    max_received_leader_id_proposal = self._get_max_received_leader_id_proposal()
                    response_kwargs = None
                    if max_received_leader_id_proposal == None:
                        response_kwargs = {
                            'nid': self.id,
                            'type': 'POTENTIAL_LEADER_ACK',
                            'value': '-1',
                        }
                    else:
                        if leader_id > max_received_leader_id_proposal['leader_id']:
                            response_kwargs = {
                                'nid': self.id,
                                'type': 'POTENTIAL_LEADER_ACK',
                                'value': max_received_leader_id_proposal['chosen_v'],
                            }
                    if response_kwargs is not None:
                        self._send_with_delay(response_kwargs, kwargs['nid'])
                elif kwargs['type'] == 'V_PROPOSE':
                    leader_id, chosen_v = kwargs['value'].split(',')
                    self.received_proposals.append({
                        'leader_id': leader_id,
                        'chosen_v': chosen_v,
                    })
                    response_kwargs = {
                        'nid': self.id,
                        'type': 'V_PROPOSE_ACK',
                        'value': '-1'
                    }
                    self._send_with_delay(response_kwargs, kwargs['nid'])
                elif kwargs['type'] == 'V_DECIDE':
                    self.agreed_value = kwargs['value']
                    break
            else:
                if kwargs['type'] == 'V_DECIDE':
                    self.agreed_value = kwargs['value']
                    break
                if kwargs['type'] == 'POTENTIAL_LEADER':
                    leader_id = kwargs['value']
                    if leader_id <= self.leader_id:
                        continue
                    self.status = 'only-responder'
                    max_received_leader_id_proposal = self._get_max_received_leader_id_proposal()
                    response_kwargs = None
                    if max_received_leader_id_proposal == None:
                        response_kwargs = {
                            'nid': self.id,
                            'type': 'POTENTIAL_LEADER_ACK',
                            'value': '-1',
                        }
                    else:
                        if leader_id > max_received_leader_id_proposal['leader_id']:
                            response_kwargs = {
                                'nid': self.id,
                                'type': 'POTENTIAL_LEADER_ACK',
                                'value': max_received_leader_id_proposal['chosen_v'],
                            }
                    if response_kwargs is not None:
                        self._send_with_delay(response_kwargs, kwargs['nid'])
                elif self.status == 'voting-p1':
                    if kwargs['type'] == 'POTENTIAL_LEADER_ACK':
                        self.received_potential_leader_acks.append(kwargs['value'])
                elif self.status == 'voting-p2':
                    self.voting_count_p2 += 1

    def start(self):
        global nodes

        time.sleep(self.submission_delay)

        self.status = 'voting-p1'
        max_received_leader_id_proposal = self._get_max_received_leader_id_proposal()
        if max_received_leader_id_proposal is None:
            self.leader_id = '1'
        else:
            self.leader_id = string_number_plus_1(max_received_leader_id_proposal['leader_id'])
        
        voting_p1_kwargs = {
            'nid': self.id,
            'type': 'POTENTIAL_LEADER',
            'value': self.leader_id,
        }
        self._broadcast(voting_p1_kwargs)
        voting_p1_start_time = time.time()
        while time.time() - voting_p1_start_time < self.voting_delay_p1:
            if (len(self.received_potential_leader_acks) > len(nodes)/2):
                break
        if not (len(self.received_potential_leader_acks) > len(nodes)/2):
            self.status = 'only-responder'
            return

        self.status = 'voting-p2'
        max_received_potential_leader_acks_leader_id = self._get_max_received_potential_leader_acks_leader_id()
        if max_received_potential_leader_acks_leader_id is None:
            self.status = 'only-responder'
            return
        self.chosen_v = max_received_potential_leader_acks_leader_id if max_received_potential_leader_acks_leader_id != '-1' else str(int(self.id) * len(nodes))
        voting_p2_kwargs = {
            'nid': self.id,
            'type': 'V_PROPOSE',
            'value': f"{self.leader_id},{self.chosen_v}"
        }
        self._broadcast(voting_p2_kwargs)
        voting_p2_start_time = time.time()
        while time.time() - voting_p2_start_time < self.voting_delay_p2:
            if (self.voting_count_p2 > len(nodes)/2):
                break
        if not (self.voting_count_p2 > len(nodes)/2):
            self.status = 'only-responder'
            return

        self.agreed_value = self.chosen_v
        decide_kwargs = {
            'nid': self.id,
            'type': 'V_DECIDE',
            'value': self.chosen_v,
        }
        self._broadcast(decide_kwargs)



if __name__ == '__main__':
    n = int(input())

    for i in range(n):
        input_node_info = input().split()
        new_node = Node(
            id=input_node_info[0],
            submission_delay=float(input_node_info[1]),
            voting_delay_p1=float(input_node_info[1]),
            voting_delay_p2=float(input_node_info[1]),
        )
        nodes.append(new_node)
        node_communication_delays[new_node.id] = {}
        for j in range(n-1):
            input_communication_input = input().split()
            node_communication_delays[new_node.id][input_communication_input[0]] = float(input_communication_input[1])

    for node in nodes:
        t = Thread(target=listen, args=(node,))
        t.start()
    
    for node in nodes:
        t = Thread(target=start, args=(node,))
        t.start()
