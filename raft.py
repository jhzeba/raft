import pdb

import sys
import json
import pickle
import gevent
import random

from gevent import socket
from gevent import event


rng = random.Random()
rng.seed(0)


def election_timer():
    timeout = 5 * rng.randint(50, 100) / 100

    timer = gevent.Timeout(timeout)
    timer.start()

    return timer


def heartbeat_timer():
    timer = gevent.Timeout(1.25)
    timer.start()

    return timer


def load_nodes(conf):
    nodes = {}

    conf = json.loads(open(conf).read())

    for node_id, (raft_port, client_port) in conf['nodes'].items():
        nodes[int(node_id)] = node(raft_port, client_port)

    return nodes


def raft_recv_thread(raft):
    node = raft.nodes[raft.node_id]
    node.raft_bind()

    while True:
        data, addr = node.raft_recv()

        if 'function' not in data:
            print('invalid request from %s: %s' % (addr, data))

        else:
            rpc_id = data['rpc_id']

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

            else:
                raise RuntimeError('invalid function from %s: %s' % (addr, data))

            data['rpc_id'] = rpc_id

            node.raft_send_to(data, addr)


def client_recv_thread(raft):
    node = raft.nodes[raft.node_id]
    node.client_bind()

    while True:
        data, addr = node.client_recv()

        if raft.leader_id != raft.node_id:
            data = {'success': False, 'leader_id': raft.leader_id}

        else:
            n = len(raft.log)
            raft.log.append(log(raft.current_term, int(data['value'])))

            for e in raft.send_entries.values():
                e.set()

            while True:
                raft.commit.wait()
                raft.commit.clear()

                if n >= raft.last_applied:
                    break

            data = {'success': True}

        node.client_send_to(data, addr)


class node(object):
    def __init__(self, raft_port, client_port):
        self.raft_port = raft_port
        self.client_port = client_port

        self.raft_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        self.rpc_id = 0

    def raft_bind(self):
        self.raft_socket.bind(('127.0.0.1', self.raft_port))

    def raft_recv(self):
        data, addr = self.raft_socket.recvfrom(1024)
        data = pickle.loads(data)

        return data, addr

    def raft_send_to(self, data, addr):
        data = pickle.dumps(data)
        self.raft_socket.sendto(data, 0, addr)

    def raft_call(self, function, data):
        rpc_id = self.rpc_id
        self.rpc_id += 1

        data['function'] = function
        data['rpc_id'] = rpc_id

        data = pickle.dumps(data)
        self.raft_socket.sendto(data, 0, ('127.0.0.1', self.raft_port))

        while True:
            data, addr = self.raft_socket.recvfrom(1024)
            data = pickle.loads(data)

            if data['rpc_id'] == rpc_id:
                return data, addr

    def client_bind(self):
        self.client_socket.bind(('127.0.0.1', self.client_port))

    def client_recv(self):
        data, addr = self.client_socket.recvfrom(1024)
        data = pickle.loads(data)

        return data, addr

    def client_send_to(self, data, addr):
        data = pickle.dumps(data)
        self.client_socket.sendto(data, 0, addr)


class log(object):
    def __init__(self, term, value):
        self.term = term
        self.value = value


class raft(object):
    def __init__(self, nodes, node_id):
        self.current_term = 0
        self.voted_for = None
        self.log = []

        self.commit_index = -1
        self.last_applied = -1

        self.leader_id = None

        self.nodes = nodes
        self.node_id = node_id
        self.quorum = int(len(nodes) / 2 + 1)

        self.demoted = event.Event()
        self.commit = event.Event()
        self.send_entries = {node_id: event.Event() for node_id in nodes.keys()}
        self.reset_timer = event.Event()

    def request_vote(self, term, candidate_id, last_log_index, last_log_term):
        demote = term > self.current_term

        if demote == True:
            self.current_term = term
            self.voted_for = None

            self.demoted.set()

        if term < self.current_term:
            return {'term': self.current_term, 'vote_granted': False}

        if self.voted_for != None:
            assert(self.voted_for != candidate_id)
            return {'term': self.current_term, 'vote_granted': False}

        if len(self.log) != 0:
            if last_log_term < self.log[-1].term:
                return {'term': self.current_term, 'vote_granted': False}

            if last_log_term == self.log[-1].term and last_log_index < len(self.log):
                return {'term': self.current_term, 'vote_granted': False}

        self.voted_for = candidate_id

        return {'term': self.current_term, 'vote_granted': True}

    def append_entries(self, term, leader_id, prev_log_index, prev_log_term, entries, leader_commit_index):
        demote = term > self.current_term

        if demote == True:
            self.current_term = term
            self.voted_for = None

            self.demoted.set()

        if term < self.current_term:
            return {'term': self.current_term, 'success': False}

        self.reset_timer.set()
        self.leader_id = leader_id

        if prev_log_index != -1:
            if prev_log_index >= len(self.log):
                return {'term': self.current_term, 'success': False}

            if prev_log_term != self.log[prev_log_index].term:
                return {'term': self.current_term, 'success': False}

        for i in range(len(entries)):
            index = prev_log_index + i + 1

            if len(self.log) >= index or self.log[index].term != entries[i].term:
                self.log = self.log[:index]
                self.log.extend(entries[i:])

                break

        if leader_commit_index > self.commit_index:
            self.commit_index = min(leader_commit_index, len(self.log) - 1)

        self.apply_committed()

        return {'term': self.current_term, 'success': True}

    def apply_committed(self):
        assert(self.last_applied <= self.commit_index)

        commits_applied = False

        while self.last_applied < self.commit_index:
            print('applying value: %d' % self.log[self.last_applied + 1].value)
            self.last_applied += 1
            commits_applied = True

        return commits_applied

    def adjust_commit_index(self):
        for n in range(len(self.raft.log) - 1, self.raft.commit_index, -1):
            if self.raft.log[n].term != self.raft.current_term:
                break

            r = 1

            for j in self.match_index.values():
                if j >= n:
                    r += 1

            if r >= self.raft.quorum:
                self.raft.commit_index = n

                break


class follower(object):
    def __init__(self, raft):
        self.raft = raft
        self.raft.demoted.clear()
        self.raft.voted_for = None

    def run(self):
        while True:
            timer = election_timer()

            try:
                self.raft.reset_timer.wait()
                self.raft.reset_timer.clear()

            except gevent.timeout.Timeout:
                break

            finally:
                timer.close()


class candidate(object):
    def __init__(self, raft):
        self.raft = raft
        self.raft.demoted.clear()

        self.votes_granted = 0
        self.vote_processed = event.Event()

    def _send_vote_request(self, data, node_id, node):
        print('sending request_vote to #%d' % node_id)

        data, addr = node.raft_call('request_vote', data)

        term = data['term']
        vote_granted = data['vote_granted']

        if term > self.raft.current_term:
            assert(vote_granted == False)

            print('new leader found #%d, switching to follower...' % node_id)

            self.raft.current_term = term
            self.raft.demoted.set()

            return

        if vote_granted == True:
            print('vote granted from #%d' % node_id)
            self.votes_granted += 1

            if self.votes_granted >= self.raft.quorum:
                self.vote_processed.set()

        else:
            print('vote denied from #%d' % node_id)

    def _send_vote_requests(self):
        last_log_index = -1
        last_log_term = -1

        if len(self.raft.log) != 0:
            last_log_index = len(self.raft.log) - 1
            last_log_term = self.raft.log[last_log_index].term

        data = {
            'term': self.raft.current_term,
            'candidate_id': self.raft.node_id,
            'last_log_index': last_log_index,
            'last_log_term': last_log_term
        }

        jobs = []

        for node_id, node in self.raft.nodes.items():
            if node_id == self.raft.node_id:
                continue

            jobs.append(gevent.spawn(self._send_vote_request, data, node_id, node))

        return jobs

    def run(self):
        print('starting new term #%d' % (self.raft.current_term + 1))

        self.raft.current_term += 1
        self.raft.voted_for = self.raft.node_id
        self.votes_granted = 1

        self.vote_processed.clear()

        timer = election_timer()
        jobs = None

        try:
            jobs = self._send_vote_requests()
            gevent.wait([self.vote_processed, self.raft.demoted], count=1)

            return True

        except gevent.timeout.Timeout:
            return False

        finally:
            timer.close()
            gevent.killall(jobs)


class leader(object):
    def __init__(self, raft):
        self.raft = raft
        self.raft.demoted.clear()

        self.raft.leader_id = self.raft.node_id

        self.next_index = {node_id: len(self.raft.log) for node_id in self.raft.nodes.keys()}
        self.match_index = {node_id: -1 for node_id in self.raft.nodes.keys()}

    def _send_append_entries(self, node_id, node):
        prev_log_index = self.next_index[node_id] - 1
        prev_log_term = -1

        if prev_log_index != - 1:
            prev_log_term = self.raft.log[prev_log_index].term

        entries = []

        if len(self.raft.log) != self.next_index[node_id]:
            entries = self.raft.log[self.next_index[node_id]:]

        data = {
            'term': self.raft.current_term,
            'leader_id': self.raft.node_id,
            'prev_log_index': prev_log_index,
            'prev_log_term': prev_log_term,
            'entries': entries,
            'leader_commit_index': self.raft.commit_index
        }

        data, addr = node.raft_call('append_entries', data)

        term = data['term']
        success = data['success']

        if term > self.raft.current_term:
            print('discovered higher term %d from #%d, switching to follower...' % (term, node_id))

            self.raft.current_term = term
            self.raft.demoted.set()

        if success == True:
            self.next_index[node_id] = prev_log_index + len(entries) + 1
            self.match_index[node_id] = prev_log_index + len(entries)

        else:
            assert(self.next_index[node_id] > 0)
            self.next_index[node_id] -= 1

        self.raft.adjust_commit_index()

        if self.raft.apply_committed() == True:
            self.raft.commit.set()

            for e in self.raft.send_entries.values():
                e.set()

        return success

    def _replicator(self, node_id, node):
        while True:
            timer = heartbeat_timer()

            try:
                if self._send_append_entries(node_id, node) == False:
                    continue

                self.raft.send_entries[node_id].wait()
                self.raft.send_entries[node_id].clear()

            except gevent.Timeout:
                pass

            finally:
                timer.close()

    def run(self):
        jobs = []

        for node_id, node in self.raft.nodes.items():
            if node_id == self.raft.node_id:
                continue

            jobs.append(gevent.spawn(self._replicator, node_id, node))

        self.raft.demoted.wait()

        gevent.killall(jobs)


nodes = load_nodes(sys.argv[1])
node_id = int(sys.argv[2])

r = raft(nodes, node_id)

gevent.spawn(raft_recv_thread, r)
gevent.spawn(client_recv_thread, r)


while True:
    follower(r).run()

    print('leader timeout, promoting to candidate...')

    if candidate(r).run() == True:
        print('election won, promoting to leader...')
        leader(r).run()

        print('demoting to follower...')

    else:
        print('election lost, demoting to follower...')
