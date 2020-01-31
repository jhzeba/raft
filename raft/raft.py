
class raft(object):
    class entry(object):
        def __init__(self, term, value):
            self.term = term
            self.value = value

    def __init__(self, helper):
        self.current_term = 0
        self.voted_for = None
        self.log = []

        self.commit_index = -1
        self.last_applied = -1

        self.helper = helper

    def request_vote(self, term, candidate_id, last_log_index, last_log_term):
        if term > self.current_term:
            self.demote(term)

        if term < self.current_term:
            return {'term': self.current_term, 'vote_granted': False}

        if self.voted_for != None and self.voted_for != candidate_id:
            return {'term': self.current_term, 'vote_granted': False}

        if len(self.log) != 0:
            if last_log_term < self.log[-1].term:
                return {'term': self.current_term, 'vote_granted': False}

            if last_log_term == self.log[-1].term and last_log_index < len(self.log) - 1:
                return {'term': self.current_term, 'vote_granted': False}

        self.voted_for = candidate_id

        return {'term': self.current_term, 'vote_granted': True}

    def append_entries(self, term, leader_id, prev_log_index, prev_log_term, entries, leader_commit_index):
        if term > self.current_term:
            self.demote(term)

        if term < self.current_term:
            return {'term': self.current_term, 'success': False}

        self.helper.signal_reset_election_timer(leader_id)

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
            self.helper.append(self.log[self.last_applied + 1].value)

            self.last_applied += 1
            commits_applied = True

        return commits_applied

    def demote(self, term):
        self.current_term = term
        self.voted_for = None

        self.helper.signal_demoted()

    def append(self, value):
        n = len(self.log)

        self.log.append(raft.entry(self.current_term, value))

        self.helper.signal_send_entries()

        state = self.helper.wait_for_commit_state()

        while True:
            state = self.helper.wait_for_commit(state)

            if n >= self.last_applied:
                break

    def leader(self):
        return self.helper.leader()
