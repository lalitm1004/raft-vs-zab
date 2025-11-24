# zab impl

# Setup
1. Get all dependencies
```bash
uv sync
```
2. Build gRPC code
```
cd raft
python-grpc-tools-protoc src/zab.proto --proto_path=./src --python_out=./src --grpc_python_out=./src
```

2. Build docker image
```bash
docker build -t zab-node .
```

3. Run benchmark
```bash
uv run src/benchmark.py
```