import docker
import os
import grpc
import time
import subprocess
import traceback
import random
import tomli
import counter_pb2
import counter_pb2_grpc

client = docker.from_env()

# ------------------ Load constants from TOML ------------------ #
with open("config.toml", "rb") as f:
    config = tomli.load(f)

N = config["N"]
START_PORT = config["START_PORT"]
IMAGE = config["IMAGE"]
BINARY_PATH = config["BINARY_PATH"]
REQUEST_INTERVAL_TIME = config["REQUEST_INTERVAL_TIME"]
WRITE_PROBABILITY = config["WRITE_PROBABILITY"]
N_REQUESTS = config["N_REQUESTS"]
SERVER_CONFIG = config.get("server", {})


# ------------------ Docker Environment ------------------ #
class Environment:
    def __init__(self, n, start_port, image, host_binary_path):
        self.image = image
        self.n_containers = n
        self.containers = {}
        self.host_binary_path = host_binary_path
        self.start_port = start_port
        self.leader_port = self.start_port

    def start(self):
        for id in range(self.n_containers):
            external_port = self.start_port + id
            container = Container(
                image=self.image,
                external_port=external_port,
                name=f"SERVER_{id}",
                host_binary_path=self.host_binary_path,
                cli_config=SERVER_CONFIG,
                id=id,
                cluster_size=self.n_containers,
            )
            self.containers[external_port] = container

        print(
            "ALL CONTAINERS STARTED WITH ADDRESSES:",
            [
                f"{address}:{container.name}"
                for address, container in self.containers.items()
            ],
        )

    def force_takedown(self):
        for container in self.containers.values():
            container.takedown(remove=True)


# ------------------ Container Wrapper ------------------ #
class Container:
    def __init__(
        self, external_port, name, image, host_binary_path, cli_config, id, cluster_size
    ):
        self.address = f"localhost:{external_port}"
        self.name = name
        self.image = image
        self.cli_config = cli_config
        self.id = id
        self.cluster_size = cluster_size

        self.container = client.containers.run(
            image,
            detach=True,
            tty=True,
            command="sleep infinity",
            name=self.name,
            network="host",
        )
        print(f"STARTED {self.name} ON HOST PORT {external_port}")

        self.container_binary_path = "/usr/local/bin/run_server"
        self.copy_bin(host_binary_path)
        self.run_server()

    def takedown(self, remove=False):
        self.container.kill()
        print(f"CONTAINER {self.name} KILLED")
        if remove:
            self.container.remove()
            print(f"CONTAINER {self.name} REMOVED")

    def revive(self):
        self.container.start()
        time.sleep(3)
        print(f"CONTAINER {self.name} REVIVED")
        self.run_server()

    def copy_bin(self, host_binary_path):
        subprocess.run(
            [
                "docker",
                "cp",
                host_binary_path,
                f"{self.name}:{self.container_binary_path}",
            ],
            check=True,
        )
        subprocess.run(
            ["docker", "exec", self.name, "chmod", "+x", self.container_binary_path],
            check=True,
        )
        print("COPIED BIN FILE")

    def run_server(self):
        args = [
            self.container_binary_path,
            f"--id={self.id}",
            f"--cluster_size={self.cluster_size}",
            f"--ip={self.cli_config.get('ip', '127.0.0.1')}",
            f"--base_port={self.address.split(':')[1]}",
            f"--election_timeout_min_ms={self.cli_config.get('election_timeout_min_ms', 150)}",
            f"--election_timeout_max_ms={self.cli_config.get('election_timeout_max_ms', 300)}",
            f"--heartbeat_interval_ms={self.cli_config.get('heartbeat_interval_ms', 75)}",
            f"--log_dir={self.cli_config.get('log_dir', './data/raft')}",
        ]

        subprocess.run(["docker", "exec", "-d", self.name] + args, check=True)

        print(f"SERVER {self.name} STARTED WITH ARGS: {' '.join(args)}")


# ------------------ Client Actions ------------------ #
class ClientActions:
    def __init__(self, env, rpc_timeout=2.0):
        time.sleep(5)
        self.environment = env
        self.rpc_timeout = rpc_timeout
        self.set_leader_addr()
        print("CLIENT INITIALIZED")

    def get(self):
        try:
            request = counter_pb2.GetRequest()
            response = self.stub.Get(request, timeout=self.rpc_timeout)

            if response.error:
                print(f"ERROR FROM {self.address}: {response.error}")
            elif response.not_leader.leader_port:
                self.set_leader_addr(response.not_leader.leader_port)
                self.get()
            else:
                print(f"FROM REQUEST [GET]: VALUE = {response.value}")

        except grpc.RpcError as e:
            print(f"RPC ERROR ON {self.address}: {e}")
            self.set_leader_addr()
            self.get()

    def increment(self, amount):
        try:
            request = counter_pb2.IncrementRequest(amount=amount)
            response = self.stub.Increment(request, timeout=self.rpc_timeout)

            if response.error:
                print(f"ERROR FROM {self.address}: {response.error}")
            elif response.not_leader.leader_port:
                self.set_leader_addr(response.not_leader.leader_port)
                self.increment(amount)
            else:
                print(f"FROM REQUEST [INCREMENT]: VALUE = {response.value}")

        except grpc.RpcError as e:
            print(f"RPC ERROR ON {self.address}: {e}")
            self.set_leader_addr()
            self.increment(amount)

    def set(self, value):
        try:
            request = counter_pb2.SetRequest(value=value)
            response = self.stub.Set(request, timeout=self.rpc_timeout)

            if response.error:
                print(f"ERROR FROM {self.address}: {response.error}")
            elif response.not_leader.leader_port:
                self.set_leader_addr(response.not_leader.leader_port)
                self.set(value)
            else:
                print(f"FROM REQUEST [SET]: VALUE = {response.value}")

        except grpc.RpcError as e:
            print(f"RPC ERROR ON {self.address}: {e}")
            self.set_leader_addr()
            self.set(value)

    def set_leader_addr(self, port=None):
        if port is None or port not in self.environment.containers:
            port = random.choice(tuple(self.environment.containers.keys()))

        self.environment.leader_port = port
        self.address = f"localhost:{port}"
        self.channel = grpc.insecure_channel(self.address)
        self.stub = counter_pb2_grpc.CounterServiceStub(self.channel)

        print(f"CORRECTING LEADER/ATTEMPTING WITH A NEW ONE: {port}")


# ------------------ Main ------------------ #
def main():
    env = Environment(
        n=N, start_port=START_PORT, image=IMAGE, host_binary_path=BINARY_PATH
    )

    try:
        env.start()
        client_actions = ClientActions(env)

        # -------- Request Loop -------- #
        for i in range(N_REQUESTS):
            is_write = random.random() < WRITE_PROBABILITY

            if is_write:
                if random.random() < 0.5:
                    value = random.randint(1, 100)
                    print(f"\nREQUEST {i+1}: SET {value}")
                    client_actions.set(value)
                else:
                    amount = random.randint(1, 10)
                    print(f"\nREQUEST {i+1}: INCREMENT {amount}")
                    client_actions.increment(amount)
            else:
                print(f"\nREQUEST {i+1}: GET")
                client_actions.get()

            time.sleep(REQUEST_INTERVAL_TIME)

    except Exception as e:
        env.force_takedown()
        traceback.print_exc()
    finally:
        env.force_takedown()


if __name__ == "__main__":
    main()
