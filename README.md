# raft-vs-zab

This repository contains the code for the benchmarking of Raft and ZAB.

## Requirements
1. A linux machine
2. `grpcio-tools` installed
3. Python
4. Docker
5. Protoc

## Setup
1. Clone repo
```bash
git clone https://github.com/lalitm1004/raft-vs-zab.git
cd raft-vs-zab
```

2. Compile gRPC code
```bash
make proto
```

3. Build docker images
```bash
cd raft
docker build -t raft-node .
cd ../zab
docker build -t raft-node.
```

4. Install requirements
```bash
cd raft
pip install -r requirements.txt # or uv sync
cd ../zab
pip install -r requirements.txt # or uv sync
```

4. Edit `config.toml` and run benchmark
```bash
cd raft # or zab
python3 src/benchmark.py # or zab
```