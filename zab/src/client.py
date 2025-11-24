import grpc
import time
import random
import tomllib
from dataclasses import dataclass
from typing import Optional

import zab_pb2_grpc as pb2_grpc
import zab_pb2 as pb2


@dataclass
class ZabConfig:
    cluster_size: int
    base_ip: str
    base_port: int
    request_delay: float = 0.0

    @classmethod
    def from_toml(cls, path: str) -> "ZabConfig":
        with open(path, "rb") as f:
            data = tomllib.load(f)
            return cls(
                cluster_size=data["cluster_size"],
                base_ip=data["base_ip"],
                base_port=data["base_port"],
            )


class ZabClient:
    def __init__(self, config_path: str):
        self.config = ZabConfig.from_toml(config_path)
        self.channel: Optional[grpc.Channel] = None
        self.stub: Optional[pb2_grpc.ZabNodeStub] = None
        self.current_node_addr: str = ""

    def get_node_address(self, node_id: int) -> str:
        return f"{self.config.base_ip}:{self.config.base_port + node_id}"

    def connect_any(self):
        """Connect to a random node in the cluster."""
        node_id = random.randint(0, self.config.cluster_size - 1)
        self.current_node_addr = self.get_node_address(node_id)
        print(f"Picked random node: {self.current_node_addr}")
        self.connect_to(self.current_node_addr)

    def connect_to(self, address: str):
        print(f"Connecting to {address}...")
        if self.channel:
            self.channel.close()
        self.channel = grpc.insecure_channel(address)
        self.stub = pb2_grpc.ZabNodeStub(self.channel)
        self.current_node_addr = address

    def set_val(self, key: str, value: str):
        if not self.stub:
            self.connect_any()

        try:
            print(f"> SetVal({key}, {value}) -> {self.current_node_addr}")
            resp = self.stub.SetVal(pb2.SetRequest(key=key, value=value), timeout=2.0)
            self._handle_response(resp, is_set=True)
        except grpc.RpcError as e:
            print(f"  [ERROR] RPC Error: {e.code()}")
            self.reset_connection()
        except Exception as e:
            print(f"  [ERROR] Unexpected error: {e}")
            self.reset_connection()

    def get_val(self, key: str):
        if not self.stub:
            self.connect_any()

        try:
            print(f"> GetVal({key}) -> {self.current_node_addr}")
            resp = self.stub.GetVal(pb2.GetRequest(key=key), timeout=2.0)
            self._handle_response(resp, is_set=False)
        except grpc.RpcError as e:
            print(f"  [ERROR] RPC Error: {e.code()}")
            self.reset_connection()
        except Exception as e:
            print(f"  [ERROR] Unexpected error: {e}")
            self.reset_connection()

    def _handle_response(self, resp, is_set: bool):
        if not resp.success and resp.leader_address:
            print(f"  [REDIRECT] Leader is at {resp.leader_address}")
            self.connect_to(resp.leader_address)
            return

        if resp.success:
            if is_set:
                print("  [SUCCESS] Set OK")
            else:
                print(f"  [SUCCESS] Value: {resp.value}")
        else:
            print("  [FAILURE] Operation failed (success=False)")

    def reset_connection(self):
        self.current_node_addr = ""
        self.stub = None
        if self.channel:
            self.channel.close()
            self.channel = None
        time.sleep(1)


def main():
    client = ZabClient("config.toml")
    print("Starting automated client...")

    try:
        while True:
            key = f"key_{random.randint(0, 20)}"
            is_set = random.choice([True, False])

            if is_set:
                value = f"val_{random.randint(0, 1000)}"
                client.set_val(key, value)
            else:
                client.get_val(key)

            time.sleep(client.config.request_delay)

    except KeyboardInterrupt:
        print("\nStopping client...")


if __name__ == "__main__":
    main()
