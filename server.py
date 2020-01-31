import sys
import json
import pickle
import gevent
import random
import raft

from gevent import socket
from gevent import event


debug_proto = False


class channel(object):
    def __init__(self, port):
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def bind(self):
        self.socket.bind(('127.0.0.1', self.port))

    def recv(self):
        data, addr = self.socket.recvfrom(1024)
        data = pickle.loads(data)

        return data, addr

    def send(self, data, addr):
        data = pickle.dumps(data)
        self.socket.sendto(data, 0, addr)


class node(channel):
    def __init__(self, port):
        channel.__init__(self, port)

        self.rpc_id = 0

    def _call(self, function, **args):
        rpc_id = self.rpc_id
        self.rpc_id += 1

        args['function'] = function
        args['rpc_id'] = rpc_id

        if debug_proto == True:
            print('> send:', args, self.port)

        self.send(args, ('127.0.0.1', self.port))

        while True:
            data, addr = self.recv()

            if data['rpc_id'] == rpc_id:
                if debug_proto == True:
                    print('> recv:', data, self.port)

                return data

    def request_vote(self, term, candidate_id, last_log_index, last_log_term, timeout):
        with gevent.Timeout(timeout, False):
            data = self._call('request_vote',
                              term=term,
                              candidate_id=candidate_id,
                              last_log_index=last_log_index,
                              last_log_term=last_log_term)

            return data['term'], data['vote_granted']

        return None, None

    def append_entries(self, term, leader_id, prev_log_index, prev_log_term, entries, leader_commit_index, timeout):
        with gevent.Timeout(timeout, False):
            data = self._call('append_entries',
                              term=term,
                              leader_id=leader_id,
                              prev_log_index=prev_log_index,
                              prev_log_term=prev_log_term,
                              entries=entries,
                              leader_commit_index=leader_commit_index)

            return data['term'], data['success']

        return None, None


class raft_helper(object):
    class n2n_event(object):
        def __init__(self):
            self.event = event.Event()
            self.state = 0

        def wait(self, state, timeout=None):
            if self.state != state:
                return self.state

            self.event.wait(timeout)
            self.event.clear()

            return self.state

        def set(self):
            self.state += 1
            self.event.set()

    def __init__(self, node_id, nodes, timeout):
        self.rng = random.Random()
        self.rng.seed(0)

        self.leader_id = None

        self.nodes = nodes
        self.node_id = node_id
        self.quorum = int(len(nodes) / 2 + 1)

        self.timeout = timeout

        self.reset_election_timer = event.Event()
        self.demoted = event.Event()
        self.election_won = event.Event()

        self.commit = raft_helper.n2n_event()
        self.send_entries = raft_helper.n2n_event()

    def _election_timeout(self):
        return self.timeout * self.rng.randint(60, 100) / 100

    def _heartbeat_timeout(self):
        return self.timeout / 3

    def clear(self, leader_id=None):
        self.demoted.clear()
        self.leader_id = leader_id

    def leader(self):
        return self.leader_id

    def run_jobs(self, function, *args):
        jobs = []

        for node_id in self.nodes.keys():
            if node_id == self.node_id:
                continue

            jobs.append(gevent.spawn(function, node_id, *args))

        return jobs

    def kill_jobs(self, jobs):
        gevent.killall(jobs)

    def signal_demoted(self):
        self.demoted.set()

    def wait_for_demoted(self):
        self.demoted.wait()

    def is_demoted(self):
        return self.demoted.is_set()

    def signal_reset_election_timer(self, leader_id):
        self.reset_election_timer.set()
        self.leader_id = leader_id

    def wait_for_reset_election_timer(self):
        try:
            self.reset_election_timer.wait(self._election_timeout())
            return self.reset_election_timer.is_set()

        finally:
            self.reset_election_timer.clear()

    def signal_election_won(self):
        self.election_won.set()

    def wait_for_election_won(self):
        try:
            gevent.wait([self.election_won, self.demoted], self._election_timeout(), 1)
            return self.election_won.is_set()

        finally:
            self.election_won.clear()

    def signal_send_entries(self):
        self.send_entries.set()

    def wait_for_send_entries(self, counter):
        return self.send_entries.wait(counter, self._heartbeat_timeout())

    def wait_for_send_entries_state(self):
        return self.send_entries.state

    def signal_commit(self):
        self.commit.set()

    def wait_for_commit(self, counter):
        return self.commit.wait(counter)

    def wait_for_commit_state(self):
        return self.commit.state

    def request_vote(self, node_id, term, candidate_id, last_log_index, last_log_term):
        node = self.nodes[node_id]

        return node.request_vote(term,
                                 candidate_id,
                                 last_log_index,
                                 last_log_term,
                                 self._heartbeat_timeout())

    def append_entries(self, node_id, term, leader_id, prev_log_index, prev_log_term, entries, leader_commit_index):
        node = self.nodes[node_id]

        return node.append_entries(term,
                                   leader_id,
                                   prev_log_index,
                                   prev_log_term,
                                   entries,
                                   leader_commit_index,
                                   self._heartbeat_timeout())

    def append(self, value):
        print('applying value: %d' % value)


def raft_thread(raft, node):
    node.bind()

    while True:
        data, addr = node.recv()

        rpc_id = data['rpc_id']

        if debug_proto == True:
            print('< recv:', data, addr)

        if data['function'] == 'request_vote':
            data = raft.request_vote(data['term'],
                                     data['candidate_id'],
                                     data['last_log_index'],
                                     data['last_log_term'])

        elif data['function'] == 'append_entries':
            data = raft.append_entries(data['term'],
                                       data['leader_id'],
                                       data['prev_log_index'],
                                       data['prev_log_term'],
                                       data['entries'],
                                       data['leader_commit_index'])

        data['rpc_id'] = rpc_id

        if debug_proto == True:
            print('< send:', data, addr)

        node.send(data, addr)


def rpc_thread(raft, rpc):
    rpc.bind()

    while True:
        data, addr = rpc.recv()

        if debug_proto == True:
            print('= recv:', data, addr)

        if raft.leader() != node_id:
            data = {'success': False, 'leader_id': raft.leader()}

        else:
            raft.append(data['value'])

            data = {'success': True, 'leader_id': raft.leader()}

        if debug_proto == True:
            print('= send:', data, addr)

        rpc.send(data, addr)


def load_nodes(conf):
    nodes = {}
    rpc = {}

    conf = json.loads(open(conf).read())

    for node_id, (raft_port, rpc_port) in conf['nodes'].items():
        rpc[int(node_id)] = channel(rpc_port)
        nodes[int(node_id)] = node(raft_port)

    return nodes, rpc


nodes, rpc = load_nodes(sys.argv[1])
node_id = int(sys.argv[2])

h = raft_helper(node_id, nodes, 5)
r = raft.raft(h)

gevent.spawn(raft_thread, r, nodes[node_id])
gevent.spawn(rpc_thread, r, rpc[node_id])

raft.run(r, h)
