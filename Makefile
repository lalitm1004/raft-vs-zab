.PHONY: proto proto-raft proto-zab clean help

help:
	@echo "Available targets:"
	@echo "  make proto       - Compile both raft and zab protobufs"
	@echo "  make proto-raft  - Compile only raft protobufs"
	@echo "  make proto-zab   - Compile only zab protobufs"
	@echo "  make clean       - Remove generated protobuf files"

proto: proto-raft proto-zab

proto-raft:
	@echo "Compiling Raft protobuf..."
	python-grpc-tools-protoc raft/src/raft.proto --proto_path=./raft/src --python_out=./raft/src --grpc_python_out=./raft/src
	@echo "Raft protobuf compiled"
	@echo ""

proto-zab:
	@echo "Compiling ZAB protobuf..."
	python-grpc-tools-protoc zab/src/zab.proto --proto_path=./zab/src --python_out=./zab/src --grpc_python_out=./zab/src
	@echo "ZAB protobuf compiled"
	@echo ""

clean:
	@echo "Cleaning generated protobuf files..."
	rm -f raft/src/*_pb2.py raft/src/*_pb2_grpc.py
	rm -f zab/src/*_pb2.py zab/src/*_pb2_grpc.py
	@echo "Cleaned"
