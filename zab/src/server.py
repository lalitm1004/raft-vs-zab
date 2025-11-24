import sys
import random
import concurrent.futures
import threading
import time
import grpc
import argparse
from dataclasses import dataclass, field
from typing import List, Dict, Tuple, Optional, Any, Union

import zab_pb2_grpc as pb2_grpc
import zab_pb2 as pb2

# Constants
HEARTBEAT_DURATION = 50  # milliseconds
ELECTION_DURATION_FROM = 150  # milliseconds
ELECTION_DURATION_TO = 300  # milliseconds

# ZAB States
STATE_LOOKING = "LOOKING"
STATE_FOLLOWING = "FOLLOWING"
STATE_LEADING = "LEADING"


@dataclass
class ZabState:
    """Holds the state of the ZAB node."""

    id: int
    nodes: Dict[int, Tuple[str, int, Optional[pb2_grpc.ZabNodeStub]]]
    state: str = STATE_LOOKING  # LOOKING, FOLLOWING, LEADING
    
    # Epoch management
    accepted_epoch: int = 0  # Highest epoch accepted
    current_epoch: int = 0   # Current epoch
    
    # Transaction management
    zxid: int = 0  # Current ZXID (epoch << 32 | counter)
    last_zxid: int = 0  # Last committed ZXID
    
    # Leader election
    vote_count: int = 0
    voted_for_id: int = -1
    leader_id: int = -1
    
    # Transaction log and state machine
    transactions: List[Tuple[int, Tuple[str, str]]] = field(default_factory=list)  # [(zxid, (key, value)), ...]
    hash_table: Dict[str, str] = field(default_factory=dict)
    
    # Proposal tracking (for leader)
    pending_proposals: Dict[int, int] = field(default_factory=dict)  # {zxid: ack_count}
    last_committed: int = 0  # Last committed ZXID
    
    # Election timer
    election_timer: Optional[threading.Timer] = None
    election_timeout: float = -1.0


class ZabNode:
    """Represents a single ZAB node in the cluster."""

    def __init__(
        self,
        id: int,
        nodes: Dict[int, Tuple[str, int, Optional[pb2_grpc.ZabNodeStub]]],
    ):
        """Initializes the ZabNode.

        Args:
            id: The ID of this node.
            nodes: A dictionary of all nodes in the cluster.
        """
        self.state = ZabState(id=id, nodes=nodes)
        self.lock = threading.Lock()
        self.election_event = threading.Event()
        self.heartbeat_events: Dict[int, threading.Event] = {}
        self.is_terminating = False
        self.is_suspended = False

        # Initialize heartbeat events for other nodes
        for node_id in nodes:
            if id != node_id:
                self.heartbeat_events[node_id] = threading.Event()

        self.start_time = time.time()

    def log_prefix(self) -> str:
        """Generates a log prefix with current state info."""
        time_since_start = "{:07.3f}".format(time.time() - self.start_time)
        return (
            f"{self.state.current_epoch}\t{time_since_start}\t{self.state.state}\t"
            f"[id={self.state.id} leader_id={self.state.leader_id} "
            f"zxid={self.state.zxid:016x} vote_count={self.state.vote_count}] "
        )

    def make_zxid(self, epoch: int, counter: int) -> int:
        """Creates a ZXID from epoch and counter."""
        return (epoch << 32) | counter

    def get_epoch_from_zxid(self, zxid: int) -> int:
        """Extracts epoch from ZXID."""
        return zxid >> 32

    def get_counter_from_zxid(self, zxid: int) -> int:
        """Extracts counter from ZXID."""
        return zxid & 0xFFFFFFFF

    def select_election_timeout(self) -> float:
        """Selects a random election timeout duration."""
        return random.randrange(ELECTION_DURATION_FROM, ELECTION_DURATION_TO) * 0.001

    def reset_election_timer(self) -> None:
        """Resets and restarts the election timer."""
        self.stop_election_timer()
        self.state.election_timer = threading.Timer(
            self.state.election_timeout, self.election_event.set
        )
        self.state.election_timer.start()

    def select_new_election_timeout_duration(self) -> None:
        """Selects a new election timeout duration."""
        self.state.election_timeout = self.select_election_timeout()

    def stop_election_timer(self) -> None:
        """Stops the election timer if it is running."""
        if self.state.election_timer:
            self.state.election_timer.cancel()

    def start_election(self) -> None:
        """Starts Fast Leader Election."""
        with self.lock:
            self.state.state = STATE_LOOKING
            self.state.leader_id = -1
            self.state.vote_count = 1
            self.state.voted_for_id = self.state.id
            # Increment epoch when starting new election
            self.state.current_epoch += 1

        print(f"{self.log_prefix()}Starting election")
        
        # Send FLE messages to all other nodes
        for id in self.state.nodes.keys():
            if id != self.state.id:
                t = threading.Thread(target=self.send_fle_message, args=(id,))
                t.start()

        self.reset_election_timer()

    def send_fle_message(self, target_id: int) -> None:
        """Sends a Fast Leader Election message to a peer."""
        self.ensure_connected(target_id)
        (_, _, stub) = self.state.nodes[target_id]
        if stub is None:
            return

        try:
            with self.lock:
                msg = pb2.FLEMessage(
                    node_id=self.state.id,
                    epoch=self.state.current_epoch,
                    zxid=self.state.last_zxid,
                    state=self.state.state,
                    leader_id=self.state.voted_for_id,
                )

            resp = stub.FastLeaderElection(msg, timeout=0.1)

            with self.lock:
                if self.state.state != STATE_LOOKING or self.is_suspended:
                    return

                # Compare proposals: higher epoch wins, then higher zxid, then higher node id
                if (resp.epoch > self.state.current_epoch or
                    (resp.epoch == self.state.current_epoch and resp.zxid > self.state.last_zxid) or
                    (resp.epoch == self.state.current_epoch and resp.zxid == self.state.last_zxid and resp.leader_id > self.state.voted_for_id)):
                    
                    # Update our vote
                    self.state.voted_for_id = resp.leader_id
                    self.state.current_epoch = resp.epoch
                    self.state.vote_count = 1

                elif resp.leader_id == self.state.voted_for_id:
                    # They agree with our vote
                    self.state.vote_count += 1

            if self.has_enough_votes():
                self.finalize_election()

        except grpc.RpcError:
            self.reopen_connection(target_id)

    def has_enough_votes(self) -> bool:
        """Checks if the node has received enough votes."""
        required_votes = (len(self.state.nodes) // 2) + 1
        return self.state.vote_count >= required_votes

    def finalize_election(self) -> None:
        """Finalizes the election process."""
        self.stop_election_timer()
        with self.lock:
            if self.state.state != STATE_LOOKING:
                return

            if self.has_enough_votes():
                if self.state.voted_for_id == self.state.id:
                    # We are the leader
                    self.state.state = STATE_LEADING
                    self.state.leader_id = self.state.id
                    self.state.vote_count = 0
                    self.state.voted_for_id = -1
                    
                    # Reset proposal tracking
                    self.state.pending_proposals = {}
                    
                    # Initialize ZXID with new epoch
                    epoch = self.state.current_epoch
                    counter = self.get_counter_from_zxid(self.state.last_zxid)
                    self.state.zxid = self.make_zxid(epoch, counter)

                    self.start_heartbeats()
                    print(f"{self.log_prefix()}I am the leader")
                    return
                else:
                    # Someone else is the leader
                    self.become_follower()
                    print(f"{self.log_prefix()}Following leader {self.state.leader_id}")
                    return

            # Election unsuccessful, restart
            self.state.state = STATE_LOOKING
            self.select_new_election_timeout_duration()
            self.reset_election_timer()

    def become_follower(self) -> None:
        """Transitions the node to follower state."""
        if self.state.state != STATE_FOLLOWING:
            print(f"{self.log_prefix()}I am a follower")
        self.state.state = STATE_FOLLOWING
        self.state.leader_id = self.state.voted_for_id
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
            stub = pb2_grpc.ZabNodeStub(channel)
            self.state.nodes[id] = (host, port, stub)

    def reopen_connection(self, id: int) -> None:
        """Reopens the gRPC connection to the specified node."""
        if id == self.state.id:
            raise ValueError("Shouldn't try to connect to itself")
        (host, port, stub) = self.state.nodes[id]
        channel = grpc.insecure_channel(f"{host}:{port}")
        stub = pb2_grpc.ZabNodeStub(channel)
        self.state.nodes[id] = (host, port, stub)

    def election_timeout_thread(self) -> None:
        """Thread that monitors election timeout."""
        while not self.is_terminating:
            if self.election_event.wait(timeout=0.5):
                self.election_event.clear()
                if self.is_suspended:
                    continue

                if self.state.state == STATE_LOOKING:
                    # Election timeout, try again
                    self.start_election()
                elif self.state.state == STATE_FOLLOWING:
                    # Leader heartbeat timeout
                    print(f"{self.log_prefix()}Leader heartbeat timeout")
                    self.start_election()

    def heartbeat_thread(self, target_id: int) -> None:
        """Thread for sending heartbeats to a specific node."""
        while not self.is_terminating:
            try:
                if self.heartbeat_events[target_id].wait(timeout=0.5):
                    self.heartbeat_events[target_id].clear()

                    if self.state.state != STATE_LEADING or self.is_suspended:
                        continue

                    self.ensure_connected(target_id)
                    (_, _, stub) = self.state.nodes[target_id]
                    if stub is None:
                        continue

                    # Send heartbeat as a commit with -1 zxid
                    resp = stub.CommitTransaction(
                        pb2.CommitRequest(
                            epoch=self.state.current_epoch,
                            zxid=-1,  # Heartbeat indicator
                        ),
                        timeout=0.100,
                    )

                    if self.state.state != STATE_LEADING or self.is_suspended:
                        continue

                    threading.Timer(
                        HEARTBEAT_DURATION * 0.001,
                        self.heartbeat_events[target_id].set,
                    ).start()
            except grpc.RpcError:
                self.reopen_connection(target_id)

    def propose_transaction(self, key: str, value: str) -> bool:
        """Proposes a new transaction (leader only)."""
        if self.state.state != STATE_LEADING:
            return False

        with self.lock:
            # Increment counter for new transaction
            counter = self.get_counter_from_zxid(self.state.zxid) + 1
            new_zxid = self.make_zxid(self.state.current_epoch, counter)
            self.state.zxid = new_zxid
            
            # Immediately apply to leader's state machine (optimistic)
            self.state.hash_table[key] = value
            self.state.last_committed = new_zxid
            self.state.last_zxid = new_zxid
            
            # Add to transaction log
            self.state.transactions.append((new_zxid, (key, value)))

        # Broadcast proposal asynchronously (fire and forget for better throughput)
        def broadcast_proposal():
            for node_id in self.state.nodes:
                if node_id == self.state.id:
                    continue

                try:
                    self.ensure_connected(node_id)
                    (_, _, stub) = self.state.nodes[node_id]
                    if stub is None:
                        continue

                    stub.ProposeTransaction(
                        pb2.ProposeRequest(
                            epoch=self.state.current_epoch,
                            zxid=new_zxid,
                            leader_id=self.state.id,
                            transaction=pb2.Transaction(
                                zxid=new_zxid,
                                key=key,
                                value=value,
                            ),
                        ),
                        timeout=0.050,
                    )

                except grpc.RpcError:
                    pass  # Follower will catch up via heartbeat/log replication

        # Start async broadcast
        threading.Thread(target=broadcast_proposal, daemon=True).start()
        return True

    def commit_transaction(self, zxid: int) -> None:
        """Commits a transaction (leader broadcasts commit)."""
        with self.lock:
            # Apply to state machine
            if zxid > self.state.last_committed:
                for txn_zxid, (key, value) in self.state.transactions:
                    if txn_zxid == zxid:
                        self.state.hash_table[key] = value
                        self.state.last_committed = zxid
                        self.state.last_zxid = zxid
                        break

        # Broadcast commit to all followers
        for node_id in self.state.nodes:
            if node_id == self.state.id:
                continue

            try:
                self.ensure_connected(node_id)
                (_, _, stub) = self.state.nodes[node_id]
                if stub is None:
                    continue

                stub.CommitTransaction(
                    pb2.CommitRequest(
                        epoch=self.state.current_epoch,
                        zxid=zxid,
                    ),
                    timeout=0.100,
                )
            except grpc.RpcError:
                self.reopen_connection(node_id)

    def wake_up_after_suspend(self) -> None:
        """Wakes up the node after suspension."""
        self.is_suspended = False
        if self.state.state == STATE_LEADING:
            self.start_heartbeats()
        else:
            self.reset_election_timer()


class ZabService(pb2_grpc.ZabNodeServicer):
    """gRPC service handler for ZAB operations."""

    def __init__(self, node: ZabNode):
        self.node = node

    def FastLeaderElection(self, request, context):
        """Handles Fast Leader Election messages."""
        if self.node.is_suspended:
            return pb2.FLEMessage(
                node_id=self.node.state.id,
                epoch=self.node.state.current_epoch,
                zxid=self.node.state.last_zxid,
                state=self.node.state.state,
                leader_id=self.node.state.voted_for_id,
            )

        with self.node.lock:
            # Update our epoch if we see a higher one
            if request.epoch > self.node.state.current_epoch:
                self.node.state.current_epoch = request.epoch
                if self.node.state.state != STATE_LOOKING:
                    # Higher epoch seen, start new election
                    self.node.state.state = STATE_LOOKING
                    self.node.state.vote_count = 1
                    self.node.state.voted_for_id = self.node.state.id

            # If we're not looking, we have a leader
            if self.node.state.state != STATE_LOOKING:
                return pb2.FLEMessage(
                    node_id=self.node.state.id,
                    epoch=self.node.state.current_epoch,
                    zxid=self.node.state.last_zxid,
                    state=self.node.state.state,
                    leader_id=self.node.state.leader_id,
                )

            # Compare and potentially update our vote
            if (request.epoch > self.node.state.current_epoch or
                (request.epoch == self.node.state.current_epoch and request.zxid > self.node.state.last_zxid) or
                (request.epoch == self.node.state.current_epoch and request.zxid == self.node.state.last_zxid and request.leader_id > self.node.state.voted_for_id)):
                
                self.node.state.voted_for_id = request.leader_id
                self.node.state.current_epoch = request.epoch

            return pb2.FLEMessage(
                node_id=self.node.state.id,
                epoch=self.node.state.current_epoch,
                zxid=self.node.state.last_zxid,
                state=self.node.state.state,
                leader_id=self.node.state.voted_for_id,
            )

    def ProposeTransaction(self, request, context):
        """Handles transaction proposals from the leader."""
        if self.node.is_suspended:
            return pb2.AckMessage(
                epoch=self.node.state.current_epoch,
                zxid=request.zxid,
                success=False,
            )

        self.node.reset_election_timer()

        with self.node.lock:
            # Check epoch
            if request.epoch > self.node.state.current_epoch:
                self.node.state.current_epoch = request.epoch
                self.node.become_follower()
                self.node.state.leader_id = request.leader_id

            if request.epoch < self.node.state.current_epoch:
                return pb2.AckMessage(
                    epoch=self.node.state.current_epoch,
                    zxid=request.zxid,
                    success=False,
                )

            # Accept the proposal
            self.node.state.transactions.append((
                request.zxid,
                (request.transaction.key, request.transaction.value)
            ))
            self.node.state.leader_id = request.leader_id

            return pb2.AckMessage(
                epoch=self.node.state.current_epoch,
                zxid=request.zxid,
                success=True,
            )

    def CommitTransaction(self, request, context):
        """Handles commit messages from the leader."""
        if self.node.is_suspended:
            return pb2.EmptyMessage()

        # Check if this is a heartbeat
        if request.zxid == -1:
            self.node.reset_election_timer()
            return pb2.EmptyMessage()

        self.node.reset_election_timer()

        with self.node.lock:
            # Apply transaction to state machine
            if request.zxid > self.node.state.last_committed:
                for txn_zxid, (key, value) in self.node.state.transactions:
                    if txn_zxid == request.zxid:
                        self.node.state.hash_table[key] = value
                        self.node.state.last_committed = request.zxid
                        self.node.state.last_zxid = request.zxid
                        break

        return pb2.EmptyMessage()

    def GetLeader(self, request, context):
        """Returns the current leader information."""
        if self.node.is_suspended:
            return pb2.GetLeaderReply(leader_id=-1, address="")

        if self.node.state.leader_id == -1:
            return pb2.GetLeaderReply(leader_id=-1, address="")

        (host, port, _) = self.node.state.nodes[self.node.state.leader_id]
        return pb2.GetLeaderReply(
            leader_id=self.node.state.leader_id, address=f"{host}:{port}"
        )

    def Suspend(self, request, context):
        """Suspends the node for a specified period."""
        if self.node.is_suspended:
            return pb2.EmptyMessage()

        self.node.is_suspended = True
        threading.Timer(request.period, self.node.wake_up_after_suspend).start()
        return pb2.EmptyMessage()

    def GetVal(self, request, context):
        """Gets a value from the key-value store."""
        if self.node.is_suspended:
            return pb2.GetReply(success=False)

        if self.node.state.state != STATE_LEADING:
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
        """Sets a value in the key-value store."""
        if self.node.is_suspended:
            return pb2.SetReply(success=False)

        if self.node.state.state != STATE_LEADING:
            if self.node.state.leader_id == -1:
                return pb2.SetReply(success=False)

            (host, port, _) = self.node.state.nodes[self.node.state.leader_id]
            return pb2.SetReply(success=False, leader_address=f"{host}:{port}")

        # Propose the transaction
        success = self.node.propose_transaction(request.key, request.value)
        return pb2.SetReply(success=success)


def start_server(node: ZabNode) -> grpc.Server:
    """Starts the gRPC server."""
    (ip, port, _stub) = node.state.nodes[node.state.id]
    server = grpc.server(concurrent.futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_ZabNodeServicer_to_server(ZabService(node), server)
    server.add_insecure_port(f"{ip}:{port}")
    server.start()
    return server


def main(
    id: int, nodes: Dict[int, Tuple[str, int, Optional[pb2_grpc.ZabNodeStub]]]
) -> None:
    """Main entry point for the ZAB node."""
    node = ZabNode(id, nodes)

    # Start election timeout thread
    election_th = threading.Thread(target=node.election_timeout_thread)
    election_th.start()

    # Start heartbeat threads
    heartbeat_threads = []
    for node_id in nodes:
        if id != node_id:
            t = threading.Thread(target=node.heartbeat_thread, args=(node_id,))
            t.start()
            heartbeat_threads.append(t)

    server = start_server(node)
    (host, port, _) = nodes[id]
    print(f"The server starts at {host}:{port}")
    print(f"I am looking for a leader. Epoch: 0")

    node.select_new_election_timeout_duration()
    node.reset_election_timer()

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        node.is_terminating = True
        server.stop(0)
        print("Shutting down")

        election_th.join()
        [t.join() for t in heartbeat_threads]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ZAB Server")
    parser.add_argument("--id", type=int, required=True, help="Node ID")
    parser.add_argument("--cluster-size", type=int, required=True, help="Cluster size")
    parser.add_argument("--ip", type=str, default="127.0.0.1", help="Base IP address")
    parser.add_argument("--base-port", type=int, default=6000, help="Base port")
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
        "--heartbeat-interval-ms", type=int, default=50, help="Heartbeat interval in ms"
    )

    args = parser.parse_args()

    nodes = {}
    for i in range(args.cluster_size):
        nodes[i] = (args.ip, args.base_port + i, None)

    print(f"Cluster nodes: {list(nodes)}")
    main(args.id, nodes)
