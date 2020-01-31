
class follower(object):
    def __init__(self, raft, helper):
        helper.clear()

        self.raft = raft
        self.helper = helper

    def run(self):
        while self.helper.wait_for_reset_election_timer() == True:
            pass
