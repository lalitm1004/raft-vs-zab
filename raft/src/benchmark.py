import time
import random
import tomllib
import grpc
import statistics
import sys
import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List, Dict, Tuple, Any
from rich.console import Console

sys.path.append(str(Path(__file__).parent))

import raft_pb2
import raft_pb2_grpc
from infrastructure import Environment, Container


@dataclass
class BenchmarkConfig:
    """Configuration for the benchmark run.

    Attributes:
        n_nodes (int): Number of Raft nodes in the cluster.
        start_port (int): Starting port for the nodes.
        image (str): Docker image name for the nodes.
        write_probability (float): Probability of sending a SetVal request (0.0 to 1.0).
        duration (int): Duration of the benchmark in seconds.
        fault_injection (bool): Whether to enable fault injection (killing leader).
        leader_kill_probability (float): Probability per second to kill the leader.
    """

    n_nodes: int
    start_port: int
    image: str
    write_probability: float
    duration: int
    fault_injection: bool
    leader_kill_probability: float

    @classmethod
    def from_toml(cls, path: Path) -> "BenchmarkConfig":
        """Loads configuration from a TOML file.

        Args:
            path (Path): Path to the TOML configuration file.

        Returns:
            BenchmarkConfig: A BenchmarkConfig instance populated from the file.
        """
        with path.open("rb") as f:
            data = tomllib.load(f)

        return cls(
            n_nodes=data["N"],
            start_port=data["START_PORT"],
            image=data["IMAGE"],
            write_probability=data["WRITE_PROBABILITY"],
            duration=data.get("BENCHMARK_DURATION", 30),
            fault_injection=data.get("FAULT_INJECTION_ENABLED", False),
            leader_kill_probability=data.get("LEADER_KILL_PROBABILITY", 0.1),
        )


class BenchmarkClient:
    """Client for running benchmark requests against the Raft cluster.

    Attributes:
        env (Environment): The Docker environment containing the cluster.
        config (BenchmarkConfig): Benchmark configuration.
        channel (Optional[grpc.Channel]): gRPC channel to the current leader.
        stub (Optional[raft_pb2_grpc.RaftNodeStub]): gRPC stub for the Raft node.
        leader_addr (Optional[str]): Address of the current leader.
    """

    def __init__(self, env: Environment, config: BenchmarkConfig) -> None:
        """
        Args:
            env (Environment): The Docker environment containing the cluster.
            config (BenchmarkConfig): Benchmark configuration.
        """
        self.env = env
        self.config = config
        self.channel: Optional[grpc.Channel] = None
        self.stub: Optional[raft_pb2_grpc.RaftNodeStub] = None
        self.leader_addr: Optional[str] = None
        self.connect_random()

    def connect_random(self) -> None:
        """Connects to a random node in the cluster."""
        if not self.env.containers:
            return
        port = random.choice(list(self.env.containers.keys()))
        self.connect_to(port)

    def connect_to(self, port: int) -> None:
        """Connects to a specific node.

        Args:
            port (int): The port of the node to connect to.
        """
        self.leader_addr = f"localhost:{port}"
        if self.channel:
            self.channel.close()
        self.channel = grpc.insecure_channel(self.leader_addr)
        self.stub = raft_pb2_grpc.RaftNodeStub(self.channel)

    def send_request(self) -> Tuple[float, bool]:
        """Sends a single request to the cluster.

        Sends either a SetVal or GetVal request based on the configured write probability.
        Handles leader redirection automatically.

        Returns:
            Tuple[float, bool]: A tuple containing the latency in seconds and a boolean indicating success.
        """
        start = time.time()
        try:
            if random.random() < self.config.write_probability:
                key = f"k{random.randint(0, 100)}"
                val = f"v{random.randint(0, 100)}"
                if self.stub:
                    resp = self.stub.SetVal(
                        raft_pb2.SetRequest(key=key, value=val), timeout=1.0
                    )
                else:
                    raise grpc.RpcError()
            else:
                key = f"k{random.randint(0, 100)}"
                if self.stub:
                    resp = self.stub.GetVal(raft_pb2.GetRequest(key=key), timeout=1.0)
                else:
                    raise grpc.RpcError()

            if not resp.success and resp.leader_address:
                try:
                    port = int(resp.leader_address.split(":")[-1])
                    self.connect_to(port)
                    return self.send_request()
                except (ValueError, IndexError):
                    pass

            return time.time() - start, True
        except (grpc.RpcError, Exception):
            self.connect_random()
            return time.time() - start, False


def main() -> None:
    """Runs the benchmark.

    Reads configuration from config.toml, sets up the environment,
    and runs the benchmark loop.
    """
    config_path = Path("config.toml")
    if not config_path.exists():
        print("config.toml not found")
        return

    config = BenchmarkConfig.from_toml(config_path)

    env = Environment(
        n=config.n_nodes, start_port=config.start_port, image=config.image
    )
    print("starting environment...")
    env.start()
    time.sleep(5)

    client = BenchmarkClient(env, config)

    console = Console()
    console.print(f"[bold]Starting benchmark for {config.duration}s...[/bold]")
    console.print(f"Write probability: {config.write_probability}")
    console.print(
        f"Fault injection: {config.fault_injection} (prob: {config.leader_kill_probability}/sec)"
    )

    # Table Header
    console.print("-" * 90)
    console.print(
        f"{'Time (s)':<10} | {'RPS':<10} | {'Success Rate':<15} | {'Events':<40}"
    )
    console.print("-" * 90)

    start_time = time.time()
    last_report = start_time

    latencies: List[float] = []
    request_count = 0
    success_count = 0
    total_requests = 0
    total_successes = 0

    killed_node: Optional[Dict[str, Any]] = None
    events: List[str] = []
    
    # Track timestep data for CSV export
    timestep_data: List[Dict[str, Any]] = []

    try:
        while time.time() - start_time < config.duration:
            current_time = time.time()

            if killed_node and (current_time - killed_node["time"] >= 1.0):
                container: Container = killed_node["container"]
                events.append(f"[cyan]REVIVE: Node {container.id}[/cyan]")
                container.revive()
                killed_node = None

            if config.fault_injection and not killed_node:
                if current_time - last_report >= 1.0:
                    if random.random() < config.leader_kill_probability:
                        try:
                            if client.leader_addr:
                                target_port = int(client.leader_addr.split(":")[-1])
                            else:
                                raise ValueError
                        except (ValueError, IndexError):
                            target_port = random.choice(list(env.containers.keys()))

                        if target_port in env.containers:
                            target_container = env.containers[target_port]
                            events.append(
                                f"[red]FAULT: Kill Node {target_container.id}[/red]"
                            )
                            target_container.takedown()
                            killed_node = {
                                "container": target_container,
                                "time": current_time,
                            }
                            client.connect_random()

            latency, success = client.send_request()
            latencies.append(latency)
            request_count += 1
            if success:
                success_count += 1

            if current_time - last_report >= 1.0:
                duration = current_time - last_report
                current_rps = request_count / duration
                success_rate = (
                    (success_count / request_count) * 100 if request_count > 0 else 0
                )

                total_requests += request_count
                total_successes += success_count

                # Calculate average latency for this timestep
                timestep_latencies = latencies[-(request_count):] if request_count > 0 else []
                avg_latency_ms = statistics.mean(timestep_latencies) * 1000 if timestep_latencies else 0
                
                # Store timestep data for CSV export (remove rich formatting from events)
                clean_events = [e.replace("[red]", "").replace("[/red]", "")
                               .replace("[cyan]", "").replace("[/cyan]", "") for e in events]
                timestep_data.append({
                    "timestamp": current_time,
                    "elapsed_seconds": int(current_time - start_time),
                    "rps": current_rps,
                    "success_rate_percent": success_rate,
                    "avg_latency_ms": avg_latency_ms,
                    "events": "; ".join(clean_events) if clean_events else ""
                })

                # Determine color for success rate
                sr_color = "green"
                if success_rate < 95:
                    sr_color = "yellow"
                if success_rate < 50:
                    sr_color = "red"

                event_str = ", ".join(events)

                console.print(
                    f"{int(current_time - start_time):<10} | {current_rps:<10.2f} | [{sr_color}]{success_rate:<15.1f}%[/{sr_color}] | {event_str}"
                )

                request_count = 0
                success_count = 0
                last_report = current_time
                events = []

    except KeyboardInterrupt:
        pass
    finally:
        console.print("-" * 90)
        console.print("[bold]Benchmark Finished[/bold]")
        
        # Calculate statistics
        if latencies:
            avg_lat = statistics.mean(latencies) * 1000
            sorted_latencies = sorted(latencies)
            n = len(sorted_latencies)
            p50 = sorted_latencies[int(n * 0.50)] * 1000 if n > 0 else 0
            p95 = sorted_latencies[int(n * 0.95)] * 1000 if n > 0 else 0
            p99 = sorted_latencies[int(n * 0.99)] * 1000 if n > 0 else 0
            console.print(f"Avg Latency: {avg_lat:.2f}ms")
            console.print(f"P50 Latency: {p50:.2f}ms")
            console.print(f"P95 Latency: {p95:.2f}ms")
            console.print(f"P99 Latency: {p99:.2f}ms")
        else:
            avg_lat = p50 = p95 = p99 = 0

        avg_tps = total_requests / config.duration if config.duration > 0 else 0
        avg_sr = (total_successes / total_requests) * 100 if total_requests > 0 else 0

        console.print(f"Average TPS: {avg_tps:.2f}")
        console.print(f"Average Success Rate: {avg_sr:.2f}%")
        
        # Generate CSV filename based on config
        write_prob_pct = int(config.write_probability * 100)
        fault_status = "enabled" if config.fault_injection else "disabled"
        kill_prob_pct = int(config.leader_kill_probability * 100)
        
        if config.fault_injection:
            filename_base = f"raft_write_prob_{write_prob_pct}_fault_{fault_status}_kill_prob_{kill_prob_pct}_nodes_{config.n_nodes}"
        else:
            filename_base = f"raft_write_prob_{write_prob_pct}_fault_{fault_status}_nodes_{config.n_nodes}"
        
        # Create data directory if it doesn't exist
        data_dir = Path("data")
        data_dir.mkdir(exist_ok=True)
        
        # Export timestep data to CSV
        timestep_csv_path = data_dir / f"{filename_base}_timesteps.csv"
        with open(timestep_csv_path, "w", newline="") as f:
            if timestep_data:
                writer = csv.DictWriter(f, fieldnames=["timestamp", "elapsed_seconds", "rps", "success_rate_percent", "avg_latency_ms", "events"])
                writer.writeheader()
                writer.writerows(timestep_data)
        console.print(f"\n[green]Timestep data saved to: {timestep_csv_path}[/green]")
        
        # Export summary statistics to CSV
        summary_csv_path = data_dir / f"{filename_base}_summary.csv"
        summary_data = [
            {"metric": "avg_latency_ms", "value": f"{avg_lat:.2f}"},
            {"metric": "p50_latency_ms", "value": f"{p50:.2f}"},
            {"metric": "p95_latency_ms", "value": f"{p95:.2f}"},
            {"metric": "p99_latency_ms", "value": f"{p99:.2f}"},
            {"metric": "avg_tps", "value": f"{avg_tps:.2f}"},
            {"metric": "avg_success_rate_percent", "value": f"{avg_sr:.2f}"},
            {"metric": "total_requests", "value": str(total_requests)},
            {"metric": "total_successes", "value": str(total_successes)},
            {"metric": "duration_seconds", "value": str(config.duration)}
        ]
        with open(summary_csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["metric", "value"])
            writer.writeheader()
            writer.writerows(summary_data)
        console.print(f"[green]Summary data saved to: {summary_csv_path}[/green]")

        env.force_takedown()


if __name__ == "__main__":
    main()
