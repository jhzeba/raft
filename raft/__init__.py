from raft.raft import raft
from raft.follower import follower
from raft.candidate import candidate
from raft.leader import leader


def run(raft, helper):
    while True:
        follower(raft, helper).run()

        if candidate(raft, helper).run() == True:
            leader(raft, helper).run()
