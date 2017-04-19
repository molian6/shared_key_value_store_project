This is an brief implementation of shared key-value store system from Mingzhe Wang and Lian Mo.

To test our system, please run the following commands:

open 5 terminals.
on the first terminal, start client:
pip install ruamel.yaml
python start.py -start_type 3 -index 0

on the second terminal, start master:
python start.py -start_type 1 -num_shards 1

on the rest of terminals, start shard replicas:
python start.py -start_type 2 -index 0/1/2

To send a request, just type command in terminal of client:
put key value
get key
delete key
batch num_requests
addshard server_ports

Test Case:
-get or delete keys that don't exist
-batch mode
-AddShard
-Kill primary replica
-Kill the second primary
-Send request while adding shard
-Add the third shard while adding the second shard
-Kill f+1 replicas for one shard
-start master with 0 shard


