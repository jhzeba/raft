## What?

Prototype raft implementation.

## Why?

I was looking for a raft protocol implementation to be able to include it in other projects. As I couldn't
find something that would be relatively easy to move around or extract from their respecive environments, I've
decided to create one myself. It was also a good way to understand how the protocol works.

## How?

Code is structured in such a way that makes it easy to separate raft protocol from the platform it's
running on. This should also enable easier verification of implementation's correctness later on. Protocol layer is
implemented in raft module. Besides the protocol itself, raft needs access to some platform primitives. Those are
exposed to the raft protocol layer through raft_helper class found in server.py. This file also implements simple
rpc access points in raft_thread and rpc_thread. Platform implementation is based on gevent and udp for simplicity.


raft.conf contains a simple map of node_id: (raft_port, rpc_port) and constitutes the cluster. Servers are started by:

```
$ python3 ./server.py raft.conf node_id
```

Client rpc can be invoked by:

```
$ python ./client.py rpc_port value
```

Localhost (127.0.0.1) is used for all communication.

## Status?

Implementation handles network partitions and log replication. Log persistence and compaction, membership changes and multi-threading
support is missing at the moment. Implementation also needs testing. Patches are welcome.

## Resources?

https://raft.github.io/ \
http://thesecretlivesofdata.com/raft/ \
https://github.com/ongardie/raft.tla/blob/master/raft.tla \
https://gist.github.com/jonhoo/ae65c28575b05da1b58e \
http://www.gevent.org/
