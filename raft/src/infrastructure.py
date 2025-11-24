import docker
import os
import time
import random

client = docker.from_env()


# ------------------ Docker Environment ------------------ #
class Environment:
    def __init__(self, n, start_port, image):
        self.image = image
        self.n_containers = n
        self.containers = {}
        self.start_port = start_port
        self.leader_port = self.start_port

    def start(self):
        # Ensure image exists
        try:
            client.images.get(self.image)
        except docker.errors.ImageNotFound:
            print(
                f"Image {self.image} not found. Please build it first: docker build -t {self.image} ."
            )
            exit(1)

        for id in range(self.n_containers):
            external_port = self.start_port + id
            container = Container(
                image=self.image,
                external_port=external_port,
                name=f"RAFT_SERVER_{id}",
                id=id,
                cluster_size=self.n_containers,
                base_port=self.start_port,
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
    def __init__(self, external_port, name, image, id, cluster_size, base_port):
        self.address = f"localhost:{external_port}"
        self.name = name
        self.image = image
        self.id = id
        self.cluster_size = cluster_size
        self.base_port = base_port

        # Remove existing container if it exists
        try:
            old_container = client.containers.get(self.name)
            old_container.remove(force=True)
        except docker.errors.NotFound:
            pass

        self.container = client.containers.run(
            image,
            detach=True,
            tty=True,
            name=self.name,
            network_mode="host",  # Use host networking for simplicity
            command=self.get_cmd(),
        )
        print(f"STARTED {self.name} ON HOST PORT {external_port}")

    def get_cmd(self):
        cmd = [
            "python",
            "server.py",
            "--id",
            str(self.id),
            "--cluster-size",
            str(self.cluster_size),
            "--base-port",
            str(self.base_port),
            "--ip",
            "127.0.0.1",  # Since we use host networking
        ]
        return cmd

    def takedown(self, remove=False):
        try:
            self.container.kill()
        except docker.errors.APIError as e:
            # Ignore if container is already stopped (409 Conflict) or not found (404)
            if "is not running" in str(e) or e.response.status_code == 409:
                pass
            else:
                raise e
        except docker.errors.NotFound:
            pass

        if remove:
            self.container.remove()
            print(f"CONTAINER {self.name} REMOVED")

    def revive(self):
        self.container.start()
