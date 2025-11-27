import grpc
import time
import random
import tomllib
from dataclasses import dataclass
from typing import Optional

import raft_pb2_grpc as pb2_grpc
import raft_pb2 as pb2


@dataclass
class RaftConfig:
    cluster_size: int
    base_ip: str
    base_port: int
    request_delay: float = 0.0

    @classmethod
    def from_toml(cls, path: str) -> "RaftConfig":
        with open(path, "rb") as f:
            data = tomllib.load(f)
            return cls(
                cluster_size=data["N"],
                base_ip="127.0.0.1",
                base_port=data["START_PORT"],
            )


class RaftClient:
    def __init__(self, config_path: str):
        self.config = RaftConfig.from_toml(config_path)
        self.channel: Optional[grpc.Channel] = None
        self.stub: Optional[pb2_grpc.RaftNodeStub] = None
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
        self.stub = pb2_grpc.RaftNodeStub(self.channel)
        self.current_node_addr = address

    def set_val(self, key: str, value: str, max_retries: int = 3):
        """Set a value with automatic redirection and retry logic."""
        if not self.stub:
            if not self._try_connect():
                print("  [ERROR] Could not connect to any node")
                return

        for attempt in range(max_retries):
            try:
                print(f"> SetVal({key}, {value}) -> {self.current_node_addr}")
                resp = self.stub.SetVal(
                    pb2.SetRequest(key=key, value=value), timeout=2.0
                )

                # Handle redirection
                if not resp.success and resp.leader_address:
                    print(f"  [REDIRECT] Leader is at {resp.leader_address}")
                    self.connect_to(resp.leader_address)
                    # Retry the request with the leader
                    continue

                # Handle successful response
                if resp.success:
                    print("  [SUCCESS] Set OK")
                    return
                else:
                    print("  [FAILURE] Operation failed")
                    return

            except grpc.RpcError as e:
                print(f"  [ERROR] RPC Error: {e.code()}")
                if attempt < max_retries - 1:
                    print(
                        f"  [RETRY] Attempting to reconnect... ({attempt + 1}/{max_retries})"
                    )
                    if not self._try_connect():
                        print("  [ERROR] Reconnection failed")
                        return
                else:
                    print("  [ERROR] Max retries reached")
                    self.reset_connection()
            except Exception as e:
                print(f"  [ERROR] Unexpected error: {e}")
                self.reset_connection()
                return

    def get_val(self, key: str, max_retries: int = 3):
        """Get a value with automatic redirection and retry logic."""
        if not self.stub:
            if not self._try_connect():
                print("  [ERROR] Could not connect to any node")
                return

        for attempt in range(max_retries):
            try:
                print(f"> GetVal({key}) -> {self.current_node_addr}")
                resp = self.stub.GetVal(pb2.GetRequest(key=key), timeout=2.0)

                # Handle redirection
                if not resp.success and resp.leader_address:
                    print(f"  [REDIRECT] Leader is at {resp.leader_address}")
                    self.connect_to(resp.leader_address)
                    # Retry the request with the leader
                    continue

                # Handle successful response
                if resp.success:
                    print(f"  [SUCCESS] Value: {resp.value}")
                    return
                else:
                    print("  [FAILURE] Key not found or operation failed")
                    return

            except grpc.RpcError as e:
                print(f"  [ERROR] RPC Error: {e.code()}")
                if attempt < max_retries - 1:
                    print(
                        f"  [RETRY] Attempting to reconnect... ({attempt + 1}/{max_retries})"
                    )
                    if not self._try_connect():
                        print("  [ERROR] Reconnection failed")
                        return
                else:
                    print("  [ERROR] Max retries reached")
                    self.reset_connection()
            except Exception as e:
                print(f"  [ERROR] Unexpected error: {e}")
                self.reset_connection()
                return

    def _try_connect(self, max_attempts: int = 3) -> bool:
        """Try to connect to a random node with retries."""
        for attempt in range(max_attempts):
            try:
                node_id = random.randint(0, self.config.cluster_size - 1)
                addr = self.get_node_address(node_id)
                print(
                    f"  [CONNECT] Trying node {node_id} at {addr}... (attempt {attempt + 1}/{max_attempts})"
                )
                self.connect_to(addr)
                return True
            except Exception as e:
                print(f"  [ERROR] Connection failed: {e}")
                if attempt < max_attempts - 1:
                    time.sleep(0.5 * (attempt + 1))  # Exponential backoff
        return False

    def reset_connection(self):
        """Reset the connection state."""
        self.current_node_addr = ""
        self.stub = None
        if self.channel:
            self.channel.close()
            self.channel = None


def main():
    client = RaftClient("config.toml")
    print("=" * 60)
    print("Raft Client REPL")
    print("=" * 60)
    print("Commands:")
    print("  getval <key>              - Get value for a key")
    print("  setval <key> <value>      - Set value for a key")
    print("  connect <node_id>         - Connect to specific node")
    print("  reconnect                 - Connect to a random node")
    print("  status                    - Show current connection status")
    print("  help                      - Show this help message")
    print("  exit                      - Exit the REPL")
    print("=" * 60)
    print()

    try:
        while True:
            try:
                command = input("> ").strip()

                if not command:
                    continue

                parts = command.split()
                cmd = parts[0].lower()

                if cmd == "exit" or cmd == "quit":
                    print("Goodbye!")
                    break

                elif cmd == "help":
                    print("\nCommands:")
                    print("  getval <key>              - Get value for a key")
                    print("  setval <key> <value>      - Set value for a key")
                    print("  connect <node_id>         - Connect to specific node")
                    print("  reconnect                 - Connect to a random node")
                    print(
                        "  status                    - Show current connection status"
                    )
                    print("  help                      - Show this help message")
                    print("  exit                      - Exit the REPL")
                    print()

                elif cmd == "status":
                    if client.current_node_addr:
                        print(f"Connected to: {client.current_node_addr}")
                    else:
                        print("Not connected")
                    print()

                elif cmd == "reconnect":
                    client.connect_any()
                    print()

                elif cmd == "connect":
                    if len(parts) < 2:
                        print("Usage: connect <node_id>")
                    else:
                        try:
                            node_id = int(parts[1])
                            if 0 <= node_id < client.config.cluster_size:
                                addr = client.get_node_address(node_id)
                                client.connect_to(addr)
                            else:
                                print(
                                    f"Invalid node_id. Must be between 0 and {client.config.cluster_size - 1}"
                                )
                        except ValueError:
                            print("Invalid node_id. Must be an integer.")
                    print()

                elif cmd == "getval":
                    if len(parts) < 2:
                        print("Usage: getval <key>")
                    else:
                        key = parts[1]
                        client.get_val(key)
                    print()

                elif cmd == "setval":
                    if len(parts) < 3:
                        print("Usage: setval <key> <value>")
                    else:
                        key = parts[1]
                        value = " ".join(parts[2:])  # Support values with spaces
                        client.set_val(key, value)
                    print()

                else:
                    print(
                        f"Unknown command: {cmd}. Type 'help' for available commands."
                    )
                    print()

            except EOFError:
                print("\nGoodbye!")
                break
            except Exception as e:
                print(f"Error processing command: {e}")
                print()

    except KeyboardInterrupt:
        print("\n\nStopping client...")
    finally:
        if client.channel:
            client.channel.close()


if __name__ == "__main__":
    main()
