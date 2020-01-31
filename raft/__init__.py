from raft.raft import raft
from raft.follower import follower
from raft.candidate import candidate
from raft.leader import leader


def run(raft, helper):
    while True:
        follower(raft, helper).run()

        print('leader timeout, promoting to candidate...')

        if candidate(raft, helper).run() == True:
            print('election won, promoting to leader...')
            leader(raft, helper).run()

            print('demoting to follower...')

        else:
            print('election lost, demoting to follower...')
