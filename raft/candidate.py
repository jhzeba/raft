class candidate(object):
    def __init__(self, raft, helper):
        helper.clear()

        self.raft = raft
        self.helper = helper

        self.votes_granted = 0

    def _request_vote(self, node_id, term, last_log_index, last_log_term):
        term, vote_granted = self.helper.request_vote(node_id,
                                                      term,
                                                      self.helper.node_id,
                                                      last_log_index,
                                                      last_log_term)

        if term == None:
            return False

        if term > self.raft.current_term:
            assert(vote_granted == False)
            self.raft.demote(term)

        else:
            if vote_granted == True:
                self.votes_granted += 1

            if self.votes_granted >= self.helper.quorum:
                self.helper.signal_election_won()

        return True

    def _election_thread(self, node_id, term, last_log_index, last_log_term):
        while True:
            if self.helper.is_demoted() == True:
                break

            if self._request_vote(node_id, term, last_log_index, last_log_term) == True:
                break

    def run(self):
        while True:
            self.raft.current_term += 1
            self.raft.voted_for = self.helper.node_id

            self.votes_granted = 1

            last_log_index = -1
            last_log_term = -1

            if len(self.raft.log) != 0:
                last_log_index = len(self.raft.log) - 1
                last_log_term = self.raft.log[last_log_index].term

            jobs = self.helper.run_jobs(self._election_thread,
                                        self.raft.current_term,
                                        last_log_index,
                                        last_log_term)

            try:
                if self.helper.wait_for_election_won() == True:
                    return True

                if self.helper.is_demoted() == True:
                    return False

            finally:
                self.helper.kill_jobs(jobs)
