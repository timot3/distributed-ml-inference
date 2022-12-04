from typing import Union
from ML.modeltypes import ModelType
from Node.messages import MessageType, Message
from PIL import Image

from io import BytesIO

import struct

#     QUERY_MODEL = 22  # sent to leader to query a model, and forwarded to the node
#     QUERY_MODEL_RESULT = 23  # sent from node to leader with the result of the query, and forwarded to the client


"""ML messages have the following fields:
== inherited from Message ==
1 byte for message type
4 bytes for the ip address of the sender
2 bytes for the port of the sender
4 bytes for the timestamp of the sender
== unique to ML messages ==
1 byte for the model type
1 byte for the dataset type

TODO: Add additional fields

"""

ML_BASE_FORMAT = "!I4sHI"
ml_struct = struct.Struct(ML_BASE_FORMAT)


class MLMessage(Message):
    def __init__(
        self,
        message_type: MessageType,
        ip: str,
        port: int,
        timestamp: int,
        model_type: ModelType,
        batch_id: int,
        file_list: list,
    ):
        super().__init__(message_type, ip, port, timestamp)
        self.model_type = model_type
        self.batch_id = batch_id
        self.file_list_string = ":::".join(file_list)

    def __str__(self):
        return f"{super().__str__()} dataset_type={self.dataset_type} model_type={self.model_type}"

    def serialize(self) -> bytes:
        return (
            super().serialize()
            + self.model_type.to_bytes(1, "big")
            + self.batch_id.to_bytes(1, "big")
            + self.file_list_string.to_bytes(1, "big")
        )

    @classmethod
    def deserialize(cls, data: Union[bytes, bytearray]) -> "MLMessage":
        try:
            (
                message_type,
                ip,
                port,
                timestamp,
                model_type,
                batch_id,
                file_list_string,
            ) = ml_struct.unpack_from(data)
            file_list = file_list_string.split(":::")
            return cls(
                MessageType(message_type),
                ip.decode("utf-8"),
                port,
                timestamp,
                ModelType(model_type),
                int(batch_id),
                file_list,
            )
        except struct.error:
            # print the length of the data and the data received
            print(
                f"Error deserializing MLMessage: {len(data)} received, with data: {data}"
            )
            raise struct.error("Error deserializing MLMessage")
