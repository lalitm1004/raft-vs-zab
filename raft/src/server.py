import sys
import random
import concurrent.futures
import threading
import time
import grpc
import argparse
import json
import os
from dataclasses import dataclass, field
from typing import List, Dict, Tuple, Optional, Any, Union

import raft_pb2_grpc as pb2_grpc
import raft_pb2 as pb2

# Constants
HEARTBEAT_DURATION = 50
ELECTION_DURATION_FROM = 150
ELECTION_DURATION_TO = 300


@dataclass
class RaftState:
    """Holds the state of the Raft node."""

    id: int
    nodes: Dict[int, Tuple[str, int, Optional[pb2_grpc.RaftNodeStub]]]
    type: str = "follower"
    term: int = 0
    vote_count: int = 0
    voted_for_id: int = -1
    leader_id: int = -1
    commit_idx: int = -1
    last_applied: int = -1
    logs: List[Tuple[int, Tuple[str, str, str]]] = field(default_factory=list)
    next_idx: List[int] = field(default_factory=list)
    match_idx: List[int] = field(default_factory=list)
    replicate_vote_count: int = 0
    hash_table: Dict[str, str] = field(default_factory=dict)

    election_campaign_timer: Optional[threading.Timer] = None
    election_timeout: float = -1.0


class RaftNode:
    """Represents a single Raft node in the cluster."""

    def __init__(
        self,
        id: int,
        nodes: Dict[int, Tuple[str, int, Optional[pb2_grpc.RaftNodeStub]]],
    ):
        """Initializes the RaftNode.

        Args:
            id: The ID of this node.
            nodes: A dictionary of all nodes in the cluster.
        """
        self.state = RaftState(id=id, nodes=nodes)
        self.lock = threading.Lock()
        self.election_timer_fired = threading.Event()
        self.heartbeat_events: Dict[int, threading.Event] = {}
        self.is_terminating = False
        self.is_suspended = False
        self.log_file_path = f"./data/logs/raft_state_{id}.json"

        # Initialize heartbeat events for other nodes
        for node_id in nodes:
            if id != node_id:
                self.heartbeat_events[node_id] = threading.Event()

        # Initialize indexes
        self.state.next_idx = [0] * len(nodes)
        self.state.match_idx = [-1] * len(nodes)

        self.start_time = time.time()
        
        # Load persistent state if it exists
        self.load_persistent_state()

    def log_prefix(self) -> str:
        """Generates a log prefix with current state info."""
        time_since_start = "{:07.3f}".format(time.time() - self.start_time)
        return (
            f"{self.state.term}\t{time_since_start}\t{self.state.type}\t"
            f"[id={self.state.id} leader_id={self.state.leader_id} "
            f"vote_count={self.state.vote_count} voted_for={self.state.voted_for_id}] "
        )

    def select_election_timeout(self) -> float:
        """Selects a random election timeout duration."""
        return random.randrange(ELECTION_DURATION_FROM, ELECTION_DURATION_TO) * 0.001

    def reset_election_campaign_timer(self) -> None:
        """Resets and restarts the election campaign timer."""
        self.stop_election_campaign_timer()
        self.state.election_campaign_timer = threading.Timer(
            self.state.election_timeout, self.election_timer_fired.set
        )
        self.state.election_campaign_timer.start()

    def select_new_election_timeout_duration(self) -> None:
        """Selects a new election timeout duration."""
        self.state.election_timeout = self.select_election_timeout()

    def stop_election_campaign_timer(self) -> None:
        """Stops the election campaign timer if it is running."""
        if self.state.election_campaign_timer:
            self.state.election_campaign_timer.cancel()

    def start_election(self) -> None:
        """Starts a new election."""
        with self.lock:
            self.state.type = "candidate"
            self.state.leader_id = -1
            self.state.term += 1
            # vote for ourselves
            self.state.vote_count = 1
            self.state.voted_for_id = self.state.id
            self.save_persistent_state()

        print(f"I am a candidate. Term: {self.state.term}")
        for id in self.state.nodes.keys():
            if id != self.state.id:
                t = threading.Thread(target=self.request_vote_worker_thread, args=(id,))
                t.start()

        self.reset_election_campaign_timer()

    def has_enough_votes(self) -> bool:
        """Checks if the node has received enough votes to become leader."""
        required_votes = (len(self.state.nodes) // 2) + 1
        return self.state.vote_count >= required_votes

    def has_enough_replicate_votes(self) -> bool:
        """Checks if enough nodes have replicated the log."""
        required_votes = (len(self.state.nodes) // 2) + 1
        return self.state.replicate_vote_count >= required_votes

    def finalize_election(self) -> None:
        """Finalizes the election process."""
        self.stop_election_campaign_timer()
        with self.lock:
            if self.state.type != "candidate":
                return

            if self.has_enough_votes():
                # become a leader
                self.state.type = "leader"
                self.state.leader_id = self.state.id
                self.state.vote_count = 0
                self.state.voted_for_id = -1

                for i in range(0, len(self.state.nodes)):
                    if i == self.state.id:
                        continue

                    self.state.next_idx[i] = 0
                    self.state.match_idx[i] = -1

                self.start_heartbeats()
                print("Votes received")
                print(f"I am a leader. Term: {self.state.term}")
                return

            # if election was unsuccessful
            self.become_a_follower()
            self.select_new_election_timeout_duration()
            self.reset_election_campaign_timer()

    def become_a_follower(self) -> None:
        """Transitions the node to follower state."""
        if self.state.type != "follower":
            print(f"I am a follower. Term: {self.state.term}")
        self.state.type = "follower"
        self.state.voted_for_id = -1
        self.state.vote_count = 0

    def start_heartbeats(self) -> None:
        """Triggers heartbeat events for all other nodes."""
        for id in self.heartbeat_events:
            self.heartbeat_events[id].set()

    def ensure_connected(self, id: int) -> None:
        """Ensures a gRPC connection exists to the specified node."""
        if id == self.state.id:
            raise ValueError("Shouldn't try to connect to itself")
        (host, port, stub) = self.state.nodes[id]
        if not stub:
            channel = grpc.insecure_channel(f"{host}:{port}")
            stub = pb2_grpc.RaftNodeStub(channel)
            self.state.nodes[id] = (host, port, stub)

    def reopen_connection(self, id: int) -> None:
        """Reopens the gRPC connection to the specified node."""
        if id == self.state.id:
            raise ValueError("Shouldn't try to connect to itself")
        (host, port, stub) = self.state.nodes[id]
        channel = grpc.insecure_channel(f"{host}:{port}")
        stub = pb2_grpc.RaftNodeStub(channel)
        self.state.nodes[id] = (host, port, stub)

    def request_vote_worker_thread(self, id_to_request: int) -> None:
        """Worker thread for sending RequestVote RPCs."""
        self.ensure_connected(id_to_request)
        (_, _, stub) = self.state.nodes[id_to_request]
        if stub is None:
            return

        try:
            resp = stub.RequestVote(
                pb2.VoteRequest(
                    term=self.state.term,
                    candidate_id=self.state.id,
                    last_log_index=len(self.state.logs) - 1,
                    last_log_term=self.state.logs[-1][0]
                    if len(self.state.logs) > 0
                    else -1,
                ),
                timeout=0.1,
            )

            with self.lock:
                if self.state.type != "candidate" or self.is_suspended:
                    return

                if self.state.term < resp.term:
                    self.state.term = resp.term
                    self.become_a_follower()
                    self.reset_election_campaign_timer()
                elif resp.result:
                    self.state.vote_count += 1

            if self.has_enough_votes():
                self.finalize_election()
        except grpc.RpcError:
            self.reopen_connection(id_to_request)

    def election_timeout_thread(self) -> None:
        """Thread that monitors election timeout."""
        while not self.is_terminating:
            if self.election_timer_fired.wait(timeout=0.5):
                self.election_timer_fired.clear()
                if self.is_suspended:
                    continue

                if self.state.type == "follower":
                    print("The leader is dead")
                    self.start_election()
                elif self.state.type == "candidate":
                    self.finalize_election()

    def heartbeat_thread(self, id_to_request: int) -> None:
        """Thread for sending heartbeats to a specific node."""
        while not self.is_terminating:
            try:
                if self.heartbeat_events[id_to_request].wait(timeout=0.5):
                    self.heartbeat_events[id_to_request].clear()

                    if (self.state.type != "leader") or self.is_suspended:
                        continue

                    self.ensure_connected(id_to_request)
                    (_, _, stub) = self.state.nodes[id_to_request]
                    if stub is None:
                        continue

                    resp = stub.AppendEntries(
                        pb2.AppendRequest(
                            term=self.state.term,
                            leader_id=self.state.id,
                            prev_log_index=-404,
                            prev_log_term=-404,
                            entries=None,
                            leader_commit=-404,
                        ),
                        timeout=0.100,
                    )

                    if (self.state.type != "leader") or self.is_suspended:
                        continue

                    with self.lock:
                        if self.state.term < resp.term:
                            self.reset_election_campaign_timer()
                            self.state.term = resp.term
                            self.become_a_follower()

                    threading.Timer(
                        HEARTBEAT_DURATION * 0.001,
                        self.heartbeat_events[id_to_request].set,
                    ).start()
            except grpc.RpcError:
                self.reopen_connection(id_to_request)

    def replicate_logs_thread(self, id_to_request: int) -> None:
        """Worker thread for replicating logs to a specific node."""
        if (self.state.type != "leader") or self.is_suspended:
            return

        entries = []
        idx_from = self.state.next_idx[id_to_request]
        for term, (_, key, value) in self.state.logs[idx_from:]:
            entries.append(pb2.Entry(term=term, key=key, value=value))

        try:
            self.ensure_connected(id_to_request)
            (_, _, stub) = self.state.nodes[id_to_request]
            if stub is None:
                return

            resp = stub.AppendEntries(
                pb2.AppendRequest(
                    term=self.state.term,
                    leader_id=self.state.id,
                    prev_log_index=self.state.next_idx[id_to_request] - 1,
                    prev_log_term=self.state.logs[
                        self.state.next_idx[id_to_request] - 1
                    ][0]
                    if self.state.next_idx[id_to_request] > 0
                    else -1,
                    entries=entries,
                    leader_commit=self.state.commit_idx,
                ),
                timeout=0.100,
            )

            with self.lock:
                if resp.result:
                    self.state.next_idx[id_to_request] = len(self.state.logs)
                    self.state.match_idx[id_to_request] = len(self.state.logs) - 1
                else:
                    self.state.next_idx[id_to_request] = max(
                        self.state.next_idx[id_to_request] - 1, 0
                    )
                    self.state.match_idx[id_to_request] = min(
                        self.state.match_idx[id_to_request],
                        self.state.next_idx[id_to_request] - 1,
                    )

        except grpc.RpcError:
            self.state.next_idx[id_to_request] = 0
            self.state.match_idx[id_to_request] = -1
            self.reopen_connection(id_to_request)

    def replicate_logs(self) -> None:
        """Main loop for log replication."""
        while not self.is_terminating:
            time.sleep(0.5)

            if (
                (self.state.type != "leader")
                or self.is_suspended
                or len(self.state.logs) == 0
            ):
                continue

            with self.lock:
                curr_id = self.state.id
                self.state.match_idx[self.state.id] = len(self.state.logs) - 1

            threads = []
            for node_id in self.state.nodes:
                if curr_id == node_id:
                    continue

                t = threading.Thread(target=self.replicate_logs_thread, args=(node_id,))
                t.start()
                threads.append(t)

            for thread in threads:
                thread.join()

            with self.lock:
                self.state.replicate_vote_count = 0
                for i in range(0, len(self.state.match_idx)):
                    if self.state.match_idx[i] > self.state.commit_idx:
                        self.state.replicate_vote_count += 1

                if self.has_enough_replicate_votes():
                    self.state.commit_idx += 1

                while self.state.commit_idx > self.state.last_applied:
                    self.state.last_applied += 1
                    _, key, value = self.state.logs[self.state.last_applied][1]
                    self.state.hash_table[key] = value

    def wake_up_after_suspend(self) -> None:
        """Wakes up the node after suspension."""
        self.is_suspended = False
        if self.state.type == "leader":
            self.start_heartbeats()
        else:
            self.reset_election_campaign_timer()
    
    def save_persistent_state(self) -> None:
        """Saves persistent state to disk."""
        try:
            os.makedirs(os.path.dirname(self.log_file_path), exist_ok=True)
            
            state_data = {
                "term": self.state.term,
                "voted_for_id": self.state.voted_for_id,
                "logs": self.state.logs,
            }
            with open(self.log_file_path, "w") as f:
                json.dump(state_data, f)
        except Exception as e:
            print(f"Error saving persistent state: {e}")
    
    def load_persistent_state(self) -> None:
        """Loads persistent state from disk if it exists."""
        if not os.path.exists(self.log_file_path):
            print(f"No persistent state found, starting fresh")
            return
        
        try:
            with open(self.log_file_path, "r") as f:
                state_data = json.load(f)
            
            self.state.term = state_data.get("term", 0)
            self.state.voted_for_id = state_data.get("voted_for_id", -1)
            self.state.logs = [tuple(entry) if isinstance(entry, list) else entry 
                              for entry in state_data.get("logs", [])]
            
            converted_logs = []
            for entry in self.state.logs:
                if isinstance(entry, (list, tuple)) and len(entry) == 2:
                    term, log_entry = entry
                    if isinstance(log_entry, (list, tuple)):
                        log_entry = tuple(log_entry)
                    converted_logs.append((term, log_entry))
            self.state.logs = converted_logs
            
            print(f"Loaded persistent state: term={self.state.term}, "
                  f"voted_for={self.state.voted_for_id}, logs={len(self.state.logs)} entries")
        except Exception as e:
            print(f"Error loading persistent state: {e}, starting fresh")


class RaftService(pb2_grpc.RaftNodeServicer):
    """gRPC service handler for Raft operations."""

    def __init__(self, node: RaftNode):
        self.node = node

    def RequestVote(self, request, context):
        if self.node.is_suspended:
            return pb2.ResultWithTerm(
                term=self.node.state.term, result=False
            )

        self.node.reset_election_campaign_timer()
        with self.node.lock:
            if self.node.state.term < request.term:
                self.node.state.term = request.term
                self.node.become_a_follower()

            failure_reply = pb2.ResultWithTerm(term=self.node.state.term, result=False)
            if request.term < self.node.state.term:
                return failure_reply
            elif request.last_log_index < len(self.node.state.logs) - 1:
                return failure_reply
            elif (
                len(self.node.state.logs) != 0
                and request.last_log_index == len(self.node.state.logs) - 1
                and request.last_log_term != self.node.state.logs[-1][0]
            ):
                return failure_reply
            elif (
                self.node.state.term == request.term
                and self.node.state.voted_for_id == -1
            ):
                self.node.become_a_follower()
                self.node.state.voted_for_id = request.candidate_id
                self.node.save_persistent_state()
                print(f"Voted for node {self.node.state.voted_for_id}")
                return pb2.ResultWithTerm(term=self.node.state.term, result=True)

            return failure_reply

    def AppendEntries(self, request, context):
        if self.node.is_suspended:
            return pb2.ResultWithTerm(term=self.node.state.term, result=False)

        self.node.reset_election_campaign_timer()

        with self.node.lock:
            is_heartbeat = (
                request.prev_log_index == -404
                or request.prev_log_term == -404
                or request.leader_commit == -404
            )

            if request.term > self.node.state.term:
                self.node.state.term = request.term
                self.node.become_a_follower()
            if is_heartbeat and request.term == self.node.state.term:
                self.node.state.leader_id = request.leader_id
                return pb2.ResultWithTerm(term=self.node.state.term, result=True)

            failure_reply = pb2.ResultWithTerm(term=self.node.state.term, result=False)
            if request.term < self.node.state.term:
                return failure_reply
            elif request.prev_log_index > len(self.node.state.logs) - 1:
                return failure_reply
            elif request.term == self.node.state.term:
                self.node.state.leader_id = request.leader_id

                success_reply = pb2.ResultWithTerm(
                    term=self.node.state.term, result=True
                )

                entries = []
                for entry in request.entries:
                    entries.append((entry.term, ("set", entry.key, entry.value)))

                start_idx = request.prev_log_index + 1

                logs_start = self.node.state.logs[:start_idx]
                logs_middle = self.node.state.logs[start_idx : start_idx + len(entries)]
                logs_end = self.node.state.logs[start_idx + len(entries) :]

                has_conflicts = False
                for i in range(0, len(logs_middle)):
                    if logs_middle[i][0] != entries[i][0]:
                        has_conflicts = True
                        break

                if has_conflicts:
                    self.node.state.logs = logs_start + entries
                else:
                    self.node.state.logs = logs_start + entries + logs_end
                
                self.node.save_persistent_state()

                if request.leader_commit > self.node.state.commit_idx:
                    self.node.state.commit_idx = min(
                        request.leader_commit, len(self.node.state.logs) - 1
                    )

                    while self.node.state.commit_idx > self.node.state.last_applied:
                        self.node.state.last_applied += 1
                        _, key, value = self.node.state.logs[
                            self.node.state.last_applied
                        ][1]
                        self.node.state.hash_table[key] = value

                return success_reply

            return failure_reply

    def GetLeader(self, request, context):
        if self.node.is_suspended:
            return pb2.GetLeaderReply(leader_id=-1, address="")

        if self.node.state.leader_id == -1:
            return pb2.GetLeaderReply(leader_id=-1, address="")

        (host, port, _) = self.node.state.nodes[self.node.state.leader_id]
        return pb2.GetLeaderReply(
            leader_id=self.node.state.leader_id, address=f"{host}:{port}"
        )

    def Suspend(self, request, context):
        if self.node.is_suspended:
            return pb2.EmptyMessage()

        self.node.is_suspended = True
        threading.Timer(request.period, self.node.wake_up_after_suspend).start()
        return pb2.EmptyMessage()

    def GetVal(self, request, context):
        if self.node.is_suspended:
            return pb2.GetReply(success=False)

        if self.node.state.type != "leader":
            if self.node.state.leader_id == -1:
                return pb2.GetReply(success=False)
            (host, port, _) = self.node.state.nodes[self.node.state.leader_id]
            return pb2.GetReply(
                success=False, value="None", leader_address=f"{host}:{port}"
            )

        with self.node.lock:
            value = self.node.state.hash_table.get(request.key)
            success = value is not None
            value = value if success else "None"

            return pb2.GetReply(success=True, value=value)

    def SetVal(self, request, context):
        if self.node.is_suspended:
            return pb2.SetReply(success=False)

        if self.node.state.type != "leader":
            if self.node.state.leader_id == -1:
                return pb2.SetReply(success=False)

            (host, port, _) = self.node.state.nodes[self.node.state.leader_id]
            return pb2.SetReply(success=False, leader_address=f"{host}:{port}")

        with self.node.lock:
            self.node.state.logs.append(
                (self.node.state.term, ("set", request.key, request.value))
            )
            self.node.save_persistent_state()
            return pb2.SetReply(success=True)


def start_server(node: RaftNode) -> grpc.Server:
    """Starts the gRPC server."""
    (ip, port, _stub) = node.state.nodes[node.state.id]
    server = grpc.server(concurrent.futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_RaftNodeServicer_to_server(RaftService(node), server)
    server.add_insecure_port(f"{ip}:{port}")
    server.start()
    return server


def main(
    id: int, nodes: Dict[int, Tuple[str, int, Optional[pb2_grpc.RaftNodeStub]]]
) -> None:
    """Main entry point for the Raft node."""
    node = RaftNode(id, nodes)

    election_th = threading.Thread(target=node.election_timeout_thread)
    election_th.start()

    heartbeat_threads = []
    for node_id in nodes:
        if id != node_id:
            t = threading.Thread(target=node.heartbeat_thread, args=(node_id,))
            t.start()
            heartbeat_threads.append(t)

    log_replication_th = threading.Thread(target=node.replicate_logs)
    log_replication_th.start()

    server = start_server(node)
    (host, port, _) = nodes[id]
    print(f"The server starts at {host}:{port}")
    print(f"I am a follower. Term: 0")

    node.select_new_election_timeout_duration()
    node.reset_election_campaign_timer()

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        node.is_terminating = True
        server.stop(0)
        print("Shutting down")

        election_th.join()
        [t.join() for t in heartbeat_threads]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Raft Server")
    parser.add_argument("--id", type=int, required=True, help="Node ID")
    parser.add_argument("--cluster-size", type=int, required=True, help="Cluster size")
    parser.add_argument("--ip", type=str, default="127.0.0.1", help="Base IP address")
    parser.add_argument("--base-port", type=int, default=8000, help="Base port")
    # Note: These args are currently not used to override constants to preserve exact functionality
    parser.add_argument(
        "--election-timeout-min-ms",
        type=int,
        default=150,
        help="Min election timeout in ms",
    )
    parser.add_argument(
        "--election-timeout-max-ms",
        type=int,
        default=300,
        help="Max election timeout in ms",
    )
    parser.add_argument(
        "--heartbeat-interval-ms", type=int, default=75, help="Heartbeat interval in ms"
    )
    parser.add_argument(
        "--log-dir", type=str, default="./data/raft", help="Log directory"
    )

    args = parser.parse_args()

    nodes = {}
    for i in range(args.cluster_size):
        nodes[i] = (args.ip, args.base_port + i, None)

    print(f"Cluster nodes: {list(nodes)}")
    main(args.id, nodes)
