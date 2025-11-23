import docker
import os
import grpc
import time
import subprocess
import counter_pb2
import counter_pb2_grpc
import traceback


N = 3
START_PORT = 8000
IMAGE = "debian:bookworm-slim"
BINARY_PATH = os.path.join("..", "bin", "add")

client = docker.from_env()

class Environment:
    def __init__(
        self,
        n,
        start_port,
        image,
        host_binary_path
    ):
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
            )

            self.containers[external_port] = container

        print(
            "ALL CONTAINERS STARTED WITH ADDRESSES:",
            [f"{address}:{container.name}" for address, container in self.containers.items()]
        )


    def force_takedown(self):
        for container in self.containers.values():
            container.takedown(remove=True)

class Container:
    def __init__(
        self,
        external_port,
        name,
        image,
        host_binary_path,
    ):
        self.address = f"localhost:{external_port}"
        self.name = name
        self.container = client.containers.run(
            image,
            detach=True,
            tty=True,
            command = "sleep infinity",
            name=self.name,
            network="host"
        )
        print(f"STARTED {self.name} ON HOST PORT {external_port}")

        self.container_binary_path = "/usr/local/bin/run_server"
        self.copy_bin(host_binary_path)
        
        self.dead = False

        self.run_server()


    def takedown(self, remove=False):
        self.container.kill()
        print(f"CONTAINER {self.name} KILLED")

        if remove:
            self.container.remove()
            print(f"CONTAINER {self.name} REMOVED")

    def revive(self):
        self.container.start()
        print(f"CONTAINER {self.name} REVIVED")
        self.run_server()

    def copy_bin(self, host_binary_path):
        subprocess.run([
            "docker", "cp", host_binary_path, f"{self.name}:{self.container_binary_path}"
        ], check=True)

        subprocess.run([
            "docker", "exec", self.name, "chmod", "+x", self.container_binary_path
        ], check=True)

        print("COPIED BIN FILE")

    def run_server(self):
        subprocess.run([
            "docker", "exec", "-d", self.name, self.container_binary_path         
        ], check=True)

        print("SERVER STARTED")


def main():

    env = Environment(
        n=N,
        start_port=START_PORT,
        image=IMAGE,
        host_binary_path=BINARY_PATH
    )

    try:
        env.start()
        env.containers[8000].takedown()
        env.containers[8000].revive()
    except Exception as e:
        env.force_takedown()
        traceback.print_exc()

    env.force_takedown()

if __name__ == "__main__":
    main()
