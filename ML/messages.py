from typing import Optional, Union, List
from ML.modeltypes import ModelType
from Node.messages import MessageType, Message, FileMessage

import struct

"""ML messages have the following fields:
== inherited from Message ==
1 Int for message type
4 bytes for the ip address of the sender
2 bytes for the port of the sender
4 bytes for the timestamp of the sender
== unique to ML messages ==
1 byte for the model type


TODO: Add additional fields

"""

ML_BASE_FORMAT = "!I4sHIB"
ml_struct = struct.Struct(ML_BASE_FORMAT)

"""
MLBatchSizeMessage is usedto tell the node how many files to expect in a batch.
"""
ML_BATCH_SIZE_FORMAT = ML_BASE_FORMAT + "I"
ml_batch_size_struct = struct.Struct(ML_BATCH_SIZE_FORMAT)

"""
MLScheduleBatchMessage is used to tell the node to schedule a batch of files.
It contains a long for the batch id, and a list of strings for the file names
The strings are encoded as utf-8 and separated by a ; character.
"""
ML_SCHEDULE_BATCH_FORMAT = ML_BASE_FORMAT + "Q"
ml_schedule_batch_struct = struct.Struct(ML_SCHEDULE_BATCH_FORMAT)

"""
MLBatchCompleteMessage is used to tell the node that a batch is complete.
It contains a long for the batch ID, a list of strings for the file names
that were predicted, and a list of strings for the results of the predictions.
The strings are encoded as utf-8 and separated by a ; character.
The list of results and the list of file names are in the same order.
The two lists are separated by a ::: sequence.
"""


class MLMessage(Message):
    def __init__(
        self,
        message_type: MessageType,
        ip: str,
        port: int,
        timestamp: int,
        model_type: ModelType,
    ):
        super().__init__(message_type, ip, port, timestamp)
        self.model_type = model_type

    def __str__(self):
        return f"{super().__str__()} model_type={self.model_type}"

    def serialize(self) -> bytes:
        return ml_struct.pack(
            self.message_type.value,
            self.ip.encode("utf-8"),
            self.port,
            self.timestamp,
            self.model_type.value,
        )

    @classmethod
    def deserialize(cls, data: Union[bytes, bytearray]) -> "MLMessage":
        message_type, ip, port, timestamp, model_type = ml_struct.unpack(data)
        return cls(
            MessageType(message_type),
            ip.decode(),
            port,
            timestamp,
            ModelType(model_type),
        )


class MLClientInferenceRequest(MLMessage):
    """Same as ML message, comes from the client, has an additional field called "files"
    which is a list of the files to do inference on
    """

    def __init__(
        self, ip: str, port: int, timestamp: int, model_type: ModelType, files: List[str]
    ):
        super().__init__(
            MessageType.CLIENT_INFERERNCE_REQUEST, ip, port, timestamp, model_type
        )
        self.files = files

    def __str__(self):
        return f"{super().__str__()} files={self.files}"

    def serialize(self) -> bytes:
        base_ml_message = ml_struct.pack(
            self.message_type.value,
            self.ip.encode(),
            self.port,
            self.timestamp,
            self.model_type.value,
        )
        files = ";".join(self.files)
        return base_ml_message + files.encode("utf-8")

    @classmethod
    def deserialize(cls, data: Union[bytes, bytearray]) -> "MLMessage":
        # print(data)
        # print(len(data))
        message_type, ip, port, timestamp, model_type = ml_struct.unpack(
            data[: ml_struct.size]
        )
        files = data[ml_struct.size :].decode("utf-8").split(";")
        return cls(ip, port, timestamp, ModelType(model_type), files)


class MLClientInferenceResponse(MLMessage):
    """Same as ML message, comes from the client, has an additional field called "files"
    which is a list of the files to do inference on
    and an additioanl field called "results" which is a list of the results of the inference
    """

    def __init__(
        self,
        ip: str,
        port: int,
        timestamp: int,
        model_type: ModelType,
        files: List[str],
        results: List[str],
    ):
        super().__init__(
            MessageType.CLIENT_INFERERNCE_RESPONSE, ip, port, timestamp, model_type
        )
        self.files = files
        self.results = results

    def __str__(self):
        return f"{super().__str__()} files={self.files}"

    def serialize(self) -> bytes:
        base_ml_message = ml_struct.pack(
            self.message_type.value,
            self.ip.encode("utf-8"),
            self.port,
            self.timestamp,
            self.model_type.value,
        )
        print(self.results)
        print(self.files)
        files = ";".join(self.files)
        results = ";".join(self.results)
        return (
            base_ml_message
            + files.encode("utf-8")
            + ":::".encode("utf-8")
            + results.encode("utf-8")
        )

    @classmethod
    def deserialize(cls, data: Union[bytes, bytearray]) -> "MLMessage":
        message_type, ip, port, timestamp, model_type = ml_struct.unpack(data)
        files_and_results = data[ml_struct.size :].decode("utf-8").split(":::")
        files = files_and_results[0].split(";")
        results = files_and_results[1].split(";")
        return cls(
            MessageType(message_type),
            ip,
            port,
            timestamp,
            ModelType(model_type),
            files,
            results,
        )


class MLBatchSizeMessage(MLMessage):
    def __init__(
        self,
        ip: str,
        port: int,
        timestamp: int,
        model_type: ModelType,
        batch_size: int,
    ):
        super().__init__(MessageType.SET_BATCH_SIZE, ip, port, timestamp, model_type)
        self.batch_size = batch_size

    def serialize(self) -> bytes:
        return super().serialize() + self.batch_size.to_bytes(1, "big")

    @classmethod
    def deserialize(cls, data: Union[bytes, bytearray]) -> "MLBatchSizeMessage":
        try:
            (
                message_type,
                ip,
                port,
                timestamp,
                model_type,
                batch_size,
            ) = ml_batch_size_struct.unpack(data)
            return cls(
                ip,
                port,
                timestamp,
                ModelType(model_type),
                batch_size,
            )

        except struct.error:
            # print the length of the data and the data received
            print(
                f"Error deserializing MLBatchSizeMessage: {len(data)} received, with data: {data}"
            )
            raise struct.error("Error deserializing MLBatchSizeMessage")


class MLBatchScheduleMessage(MLMessage):
    def __init__(
        self,
        ip: str,
        port: int,
        timestamp: int,
        model_type: ModelType,
        batch_id: int,
        file_names: List[str],
    ):
        super().__init__(MessageType.SCHEDULE_BATCH, ip, port, timestamp, model_type)
        self.batch_id = batch_id
        self.file_names = file_names

    def serialize(self) -> bytes:
        file_names = ";".join(self.file_names)
        return (
            super().serialize()
            + self.batch_id.to_bytes(8, "big")
            + file_names.encode("utf-8")
        )

    @classmethod
    def deserialize(cls, data: Union[bytes, bytearray]) -> "MLBatchScheduleMessage":
        try:
            (
                message_type,
                ip,
                port,
                timestamp,
                model_type,
                batch_id,
            ) = ml_schedule_batch_struct.unpack(data[: ml_schedule_batch_struct.size])
            file_names = data[ml_schedule_batch_struct.size :].decode("utf-8").split(";")
            return cls(
                ip,
                port,
                timestamp,
                ModelType(model_type),
                batch_id,
                file_names,
            )

        except struct.error:
            # print the length of the data and the data received
            print(
                f"Error deserializing MLBatchScheduleMessage: {len(data)} received, with data: {data}"
            )
            raise struct.error("Error deserializing MLBatchScheduleMessage")


class MLBatchResultMessage(MLMessage):
    def __init__(
        self,
        ip: str,
        port: int,
        timestamp: int,
        model_type: ModelType,
        batch_id: int,
        file_names: List[str],
        results: List[str],
    ):
        super().__init__(MessageType.BATCH_COMPLETE, ip, port, timestamp, model_type)
        self.batch_id = batch_id
        self.file_names = file_names
        self.results = results

    def serialize(self) -> bytes:
        file_names = ";".join(self.file_names)
        results = ";".join(self.results)
        return (
            super().serialize()
            + self.batch_id.to_bytes(8, "big")
            + file_names.encode("utf-8")
            + ":::".encode("utf-8")
            + results.encode("utf-8")
        )

    @classmethod
    def deserialize(cls, data: Union[bytes, bytearray]) -> "MLBatchResultMessage":
        try:
            (
                message_type,
                ip,
                port,
                timestamp,
                model_type,
                batch_id,
            ) = ml_schedule_batch_struct.unpack(data[: ml_schedule_batch_struct.size])
            file_names_results = data[ml_schedule_batch_struct.size :].split(b":::")
            file_names = file_names_results[0].decode("utf-8").split(";")
            results = file_names_results[1].decode("utf-8").split(";")

            res = cls(
                ip,
                port,
                timestamp,
                ModelType(model_type),
                batch_id,
                file_names,
                results,
            )
            res.message_type = message_type
            return res

        except struct.error:
            # print the length of the data and the data received
            print(
                f"Error deserializing MLBatchResultMessage: {len(data)} received, with data: {data}"
            )
            raise struct.error("Error deserializing MLBatchResultMessage")


def get_batch_complete_msg(
    server, model_type, batch_id, file_names, results: Optional[List[str]] = None
):
    msg = MLBatchResultMessage(
        server.host,
        server.port,
        server.timestamp,
        model_type,
        batch_id,
        file_names,
        results,
    )
    if results is None:
        msg.results = []
        msg.message_type = MessageType.BATCH_FAILED

    return msg


def get_file_get_msg(server, file_name: str):
    msg = FileMessage(
        MessageType.GET,
        server.host,
        server.port,
        server.timestamp,
        file_name,
        0,
        b"",
    )
    return msg
