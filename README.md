# Shared Key Value Store
- every shard has master's hashing function
- timeouts should be handled together if we choose to use udp

## Client to Master (create a new class for the message?)
- put(key, value)
- get(key)
- delete(key)
- addShard(configFile)

## Master to shards (use the old message class)
- put: send the "put(key, value)"" pair as message.value to proposer. Add to replica's dict if it is learned. Shards send acknowledge back when f+1 replica learned
- get: send "get(key)" as message.value to proposer.....then????
- delete: send the "put(key, value)"" pair as message.value to proposer. Delete from replica's dict if it is learned. Shards send acknowledge back when f+1 replica learned
