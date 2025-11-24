# ZAB (Zookeeper Atomic Broadcast) Consensus Implementation

This is a Python implementation of the ZAB (Zookeeper Atomic Broadcast) consensus protocol for distributed key-value storage. ZAB is the consensus protocol used by Apache ZooKeeper.

## Protocol Overview

ZAB is a consensus protocol designed for atomic broadcast and total ordering of messages. Key features:

- **Fast Leader Election (FLE)**: Efficient leader election based on ZXID and node IDs
- **ZXID-based Ordering**: Transactions are ordered using ZXIDs (epoch << 32 | counter)
- **Two-Phase Commit**: Propose → Acknowledge → Commit
- **Epoch Management**: Each leader has a unique epoch for transaction ordering
- **Atomic Broadcast**: All transactions are totally ordered across all nodes

### States

- **LOOKING**: Node is searching for a leader
- **FOLLOWING**: Node is following the elected leader
- **LEADING**: Node is the current leader

## Building

Build the Docker image:

```bash
cd /home/lait/devenv/projects/raft-vs-zab/zab
docker build -t zab-node .
```

## Running

### Generate Protobuf Files

```bash
cd src
python3 -m grpc_tools.protoc zab.proto --proto_path=. --python_out=. --grpc_python_out=.
```

### Run Benchmark

The benchmark will automatically start a cluster of ZAB nodes and run performance tests:

```bash
cd src
python3 benchmark.py
```

Configuration is read from `config.toml`.

### Run Interactive Client

```bash
cd src
python3 client.py
```

## Architecture

### Components

1. **server.py**: Core ZAB implementation
   - `ZabState`: Node state (epoch, zxid, transactions, etc.)
   - `ZabNode`: Main node logic (FLE, proposal handling, commit processing)
   - `ZabService`: gRPC service handlers

2. **client.py**: Client for interacting with the cluster
   - Automatic leader discovery and redirection
   - SetVal/GetVal operations

3. **infrastructure.py**: Docker orchestration
   - Environment management
   - Container lifecycle (start, kill, revive)

4. **benchmark.py**: Performance testing
   - Configurable workload (read/write ratio)
   - Fault injection (leader killing)
   - Rich console output with metrics

### Protocol Details

#### Leader Election (FLE)

1. Node enters LOOKING state
2. Sends FLE messages with (epoch, zxid, node_id)
3. Compares proposals: higher epoch wins, then higher zxid, then higher node_id
4. Node with majority votes becomes leader

#### Transaction Processing

1. Client sends SetVal to leader
2. Leader proposes transaction with new ZXID (epoch + counter)
3. Followers acknowledge proposal
4. Leader commits after receiving majority acks
5. Leader broadcasts commit to all followers

## Configuration

Edit `config.toml`:

```toml
N = 10                          # Number of nodes
START_PORT = 6000               # Starting port (6000-6009)
IMAGE = "zab-node"              # Docker image name
BENCHMARK_DURATION = 30         # Benchmark duration in seconds
FAULT_INJECTION_ENABLED = true  # Enable fault injection
LEADER_KILL_PROBABILITY = 0.5   # Probability of killing leader per second
WRITE_PROBABILITY = 0.5         # Probability of write operations
```

## Comparison with Raft

| Feature | Raft | ZAB |
|---------|------|-----|
| Leader Election | Term-based voting | Fast Leader Election (FLE) |
| Transaction ID | (term, index) | ZXID (epoch << 32 \| counter) |
| Replication | AppendEntries RPC | Propose → Ack → Commit |
| Ordering | Log index | ZXID comparison |
| States | Follower/Candidate/Leader | Looking/Following/Leading |

Both protocols achieve the same goal: linearizable state machine replication with strong consistency guarantees.
