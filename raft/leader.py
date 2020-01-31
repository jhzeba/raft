class leader(object):
    def __init__(self, raft, helper):
        helper.clear(helper.node_id)

        self.raft = raft
        self.helper = helper

        self.next_index = {node_id: len(raft.log) for node_id in helper.nodes.keys()}
        self.match_index = {node_id: -1 for node_id in helper.nodes.keys()}

    def _adjust_commit_index(self):
        for n in range(len(self.raft.log) - 1, self.raft.commit_index, -1):
            if self.raft.log[n].term != self.raft.current_term:
                break

            r = 1

            for j in self.match_index.values():
                if j >= n:
                    r += 1

            if r >= self.helper.quorum:
                self.raft.commit_index = n

                break

    def _append_entries(self, node_id):
        prev_log_index = self.next_index[node_id] - 1
        prev_log_term = -1

        if prev_log_index != - 1:
            prev_log_term = self.raft.log[prev_log_index].term

        entries = []

        if len(self.raft.log) != self.next_index[node_id]:
            entries = self.raft.log[self.next_index[node_id]:]

        term, success = self.helper.append_entries(node_id,
                                                   self.raft.current_term,
                                                   self.helper.node_id,
                                                   prev_log_index,
                                                   prev_log_term,
                                                   entries,
                                                   self.raft.commit_index)

        if term == None:
            return False

        if term > self.raft.current_term:
            print('discovered higher term %d from #%d, switching to follower...' % (term, node_id))
            self.raft.demote(term)

        if success == True:
            self.next_index[node_id] = prev_log_index + len(entries) + 1
            self.match_index[node_id] = prev_log_index + len(entries)

        else:
            assert(self.next_index[node_id] > 0)
            self.next_index[node_id] -= 1

        self._adjust_commit_index()

        if self.raft.apply_committed() == True:
            self.helper.signal_commit()
            self.helper.signal_send_entries()

        return success

    def _log_replicator_thread(self, node_id):
        state = self.helper.wait_for_send_entries_state()

        while True:
            if self.helper.is_demoted() == True:
                break

            if self._append_entries(node_id) == False:
                continue

            state = self.helper.wait_for_send_entries(state)

    def run(self):
        jobs = self.helper.run_jobs(self._log_replicator_thread)

        try:
            self.helper.wait_for_demoted()

        finally:
            self.helper.kill_jobs(jobs)
