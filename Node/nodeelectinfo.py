from threading import Lock
from typing import List, Tuple
import time


class NodeElectInfo:
    def __init__(self):
        # Timestamp of the last ELECT_PING message
        self._election_timestamp = int(time.time())
        self._election_lock = Lock()
        # Timestamp of the last time a node claimed to be leader
        self._claim_leader_timestamp = int(time.time())
        self._claim_leader_lock = Lock()
        self._pending_ack_timestamp = int(time.time())
        self._pending_ack_nodes = []
        self._pending_ack_nodes_lock = Lock()

    def get_election_timestamp(self) -> int:
        with self._election_lock:
            return self._election_timestamp

    def update_election_timestamp(self) -> None:
        with self._election_lock:
            self._election_timestamp = int(time.time())

    def get_claim_leader_timestamp(self) -> int:
        with self._claim_leader_lock:
            return self._claim_leader_timestamp

    def update_claim_leader_timestamp(self) -> None:
        with self._claim_leader_lock:
            self._claim_leader_lock = int(time.time())

    def init_pending_nodes(self, nodes: List[Tuple[str, str, int]]) -> None:
        with self._pending_ack_nodes_lock:
            self._pending_ack_timestamp = int(time.time())
            self._pending_ack_nodes = nodes

    def get_pending_ack_timestamp(self) -> int:
        with self._pending_ack_nodes_lock:
            return self._pending_ack_timestamp

    def update_pending_nodes(self, nodeid: Tuple[str, str, int], timestamp) -> bool:
        # Ignore messages from previous election rounds
        if timestamp != self._pending_ack_timestamp:
            return False
        # Otherwise remove. Node does not exist -> bug
        with self._pending_ack_nodes_lock:
            self._pending_ack_nodes.remove(nodeid)
            return len(self._pending_ack_nodes) == 0

    pass
