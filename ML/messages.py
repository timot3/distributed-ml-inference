from typing import Union
from ML.modeltypes import DatasetType, MLModelType
from Node.messages import MessageType, Message

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
    ):
        super().__init__(message_type, ip, port, timestamp)
        self.dataset_type = dataset_type
        self.model_type = model_type

    def __str__(self):
        return f"{super().__str__()} dataset_type={self.dataset_type} model_type={self.model_type}"

    def serialize(self) -> bytes:
        return (
            super().serialize()
            + self.dataset_type.to_bytes(1, "big")
            + self.model_type.to_bytes(1, "big")
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
                dataset_type,
            ) = ml_struct.unpack_from(data)
            return cls(
                MessageType(message_type),
                ip.decode("utf-8"),
                port,
                timestamp,
                DatasetType(dataset_type),
                MLModelType(model_type),
            )
        except struct.error:
            # print the length of the data and the data received
            print(
                f"Error deserializing MLMessage: {len(data)} received, with data: {data}"
            )
            raise struct.error("Error deserializing MLMessage")


class RegisterModelMessage(MLMessage):
    def __init__(
        self,
        ip: str,
        port: int,
        timestamp: int,
        dataset_type: DatasetType,
        model_type: MLModelType,
    ):
        super().__init__(
            MessageType.REGISTER_MODEL, ip, port, timestamp, dataset_type, model_type
        )

    def __str__(self):
        return f"{super().__str__()}"

    def serialize(self) -> bytes:
        return super().serialize()

    @classmethod
    def deserialize(cls, data: Union[bytes, bytearray]) -> "RegisterModelMessage":
        return cls(*super().deserialize(data)[1:])


class RegisterModelAckMessage(MLMessage):
    def __init__(
        self,
        ip: str,
        port: int,
        timestamp: int,
        dataset_type: DatasetType,
        model_type: MLModelType,
    ):
        super().__init__(
            MessageType.REGISTER_MODEL_ACK, ip, port, timestamp, dataset_type, model_type
        )

    def __str__(self):
        return f"{super().__str__()}"

    def serialize(self) -> bytes:
        return super().serialize()

    @classmethod
    def deserialize(cls, data: Union[bytes, bytearray]) -> "RegisterModelAckMessage":
        return cls(*super().deserialize(data)[1:])


class QueryModelMessage(MLMessage):
    def __init__(
        self,
        ip: str,
        port: int,
        timestamp: int,
        dataset_type: DatasetType,
        model_type: MLModelType,
        queryid: int,
    ):
        super().__init__(
            MessageType.QUERY_MODEL, ip, port, timestamp, dataset_type, model_type
        )

    def __str__(self):
        return f"{super().__str__()}"

    def serialize(self) -> bytes:
        return super().serialize()

    @classmethod
    def deserialize(cls, data: Union[bytes, bytearray]) -> "QueryModelMessage":
        return cls(*super().deserialize(data)[1:])
