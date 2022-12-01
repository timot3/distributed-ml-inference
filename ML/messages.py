from typing import Union
from ML.modeltypes import DatasetType, MLModelType
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

Tentatively, the dataset type is not being used since we have a 1 to 1
mapping between the model and dataset.
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
        dataset_type: DatasetType,
        model_type: MLModelType,
        file_idx_start: int,  # Assumes that we sort the list of filenames
        file_idx_end: int,
    ):
        super().__init__(message_type, ip, port, timestamp)
        self.dataset_type = dataset_type
        self.model_type = model_type
        self.file_idx_start = file_idx_start
        self.file_idx_end = file_idx_end

    def __str__(self):
        return f"{super().__str__()} dataset_type={self.dataset_type} model_type={self.model_type}"

    def serialize(self) -> bytes:
        return (
            super().serialize()
            + self.dataset_type.to_bytes(1, "big")
            + self.model_type.to_bytes(1, "big")
            + self.file_idx_start.to_bytes(1, "big")
            + self.file_idx_end.to_bytes(1, "big")
        )

    @classmethod
    def deserialize(cls, data: Union[bytes, bytearray]) -> "MLMessage":
        try:
            (
                message_type,
                ip,
                port,
                timestamp,
                dataset_type,
                model_type,
                file_idx_start,
                file_idx_end,
            ) = ml_struct.unpack_from(data)
            return cls(
                MessageType(message_type),
                ip.decode("utf-8"),
                port,
                timestamp,
                DatasetType(dataset_type),
                MLModelType(model_type),
                file_idx_start,
                file_idx_end,
            )
        except struct.error:
            # print the length of the data and the data received
            print(
                f"Error deserializing MLMessage: {len(data)} received, with data: {data}"
            )
            raise struct.error("Error deserializing MLMessage")


class QueryModelMessage(MLMessage):
    def __init__(
        self,
        ip: str,
        port: int,
        timestamp: int,
        dataset_type: DatasetType,
        model_type: MLModelType,
        queryid: int,
        image: Image.Image,
    ):
        super().__init__(
            MessageType.QUERY_MODEL, ip, port, timestamp, dataset_type, model_type
        )
        self.image = image

    def __str__(self):
        return f"{super().__str__()}"

    def serialize(self) -> bytes:
        # convert the image to bytes
        img_bytes = self.image.tobytes()
        # get the hash of img_bytes
        img_hash = hash(img_bytes)
        # truncate the hash to a long (8 bytes) -- this will be the queryid
        queryid = img_hash & 0xFFFFFFFFFFFFFFFF
        img_len = len(img_bytes)

        query_model_struct = struct.Struct(f"QQ{img_len}s")
        query_model_bytes = query_model_struct.pack(img_hash, img_len, img_bytes)

        return super().serialize() + query_model_bytes

    @classmethod
    def deserialize(cls, data: Union[bytes, bytearray]) -> "QueryModelMessage":
        parent_msg = super().deserialize(data)
        # get the queryid and image length
        queryid, img_len = struct.unpack_from("!QQ", data, ml_struct.size)
        # get the image bytes
        img_bytes = struct.unpack_from(f"{img_len}s", data, ml_struct.size + 16)[0]
        # convert the image bytes to an image

        img = Image.open(BytesIO(img_bytes))
        return cls(*parent_msg[1:], queryid, img)
