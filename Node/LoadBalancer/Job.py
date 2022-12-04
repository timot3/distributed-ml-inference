from typing import TYPE_CHECKING, List, Optional, Union

from ML.messages import MLMessage
from Node.messages import MessageType

if TYPE_CHECKING:
    from ML.modeltypes import MLModelType


def get_job_id_hash(files: List[str]) -> int:
    """Get a hash of the job id"""
    return hash("".join(files))


class Job:
    def __init__(self, model: MLModelType, files: List[str]):
        self.model = model
        self.files = files
        self.id = get_job_id_hash(files)
        self.result = None
        self.node_scheduled_on = None

    def __eq__(self, other):
        return self.id == other.id

    def __hash__(self):
        return self.id

    def schedule(self, node: "Member"):
        self.node_scheduled_on = node

    def complete(self, result: Union[bytes, bytearray]):
        self.result = result

    def get_result(self) -> Optional[Union[bytes, bytearray]]:
        return self.result

    def is_complete(self) -> bool:
        return self.result is not None

    def get_job_message(self) -> MLMessage:
        ip = self.node_scheduled_on.ip
        port = self.node_scheduled_on.port
        timestamp = self.node_scheduled_on.timestamp
        msg = MLMessage(MessageType.QUERY_MODEL, ip, port, timestamp, 0, self.model, 0, 1)
        return msg
