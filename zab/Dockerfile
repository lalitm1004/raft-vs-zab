FROM python:3.11-slim

WORKDIR /app

RUN pip install grpcio grpcio-tools protobuf

COPY src/server.py .
COPY src/zab.proto .

RUN python-grpc-tools-protoc zab.proto --proto_path=. --python_out=. --grpc_python_out=.
