FROM python:3.11-slim

WORKDIR /app

RUN pip install grpcio grpcio-tools protobuf

COPY src/server.py .
COPY src/raft.proto .

RUN python-grpc-tools-protoc raft.proto --proto_path=. --python_out=. --grpc_python_out=.