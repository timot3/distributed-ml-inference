import time
from typing import TYPE_CHECKING, List, Optional, Union


if TYPE_CHECKING:
    from ML.messages import MLMessage
    from ML.modeltypes import ModelType
    from Node.messages import MessageType, Message


def get_job_id_hash(files: List[str]) -> int:
    """Get a hash of the job id"""
    return abs(hash("".join(files)))


class Batch:
    def __init__(
        self, model: "ModelType", files: List[str], batch_id: Optional[int] = None
    ):
        self.model_type = model
        self.files = files
        self.id = batch_id if batch_id is not None else get_job_id_hash(files)
        self.result = None
        self.node_scheduled_on = None
        self.time_started = None
        self.time_ended = None

    def __eq__(self, other):
        return self.id == other.id

    def __hash__(self):
        return self.id

    def schedule(self, node: "Member"):
        self.node_scheduled_on = node
        self.time_started = int(time.time())

    def complete(self, result: Union[bytes, bytearray]):
        self.result = result
        self.time_ended = int(time.time())

    def get_completion_time(self) -> Optional[int]:
        if self.time_started is None or self.time_ended is None:
            return None
        return self.time_ended - self.time_started

    def get_result(self) -> Optional[Union[bytes, bytearray]]:
        return self.result

    def is_complete(self) -> bool:
        return self.result is not None

    def get_job_message(self) -> "MLBatchScheduleMessage":
        from ML.messages import MLBatchScheduleMessage

        ip = self.node_scheduled_on.ip
        port = self.node_scheduled_on.port
        timestamp = self.node_scheduled_on.timestamp
        msg = MLBatchScheduleMessage(
            ip, port, timestamp, self.model_type, self.id, self.files
        )
        return msg


class BatchResult:
    def __init__(self, batch: Batch, result: Optional["Message"]):
        self.job = batch
        self.id = batch.id
        self.files = batch.files
        self.result = result
        self.time_to_complete = batch.get_completion_time()
