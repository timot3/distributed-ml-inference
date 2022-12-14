import logging
import socket
import struct
import textwrap
import time
import traceback
from typing import Optional, Tuple, Union, List

from ML.messages import (
    MLBatchScheduleMessage,
    MLBatchSizeMessage,
    MLBatchResultMessage,
    MLClientInferenceRequest,
    MLClientInferenceResponse,
)
from ML.modeltypes import ModelType
from Node.messages import (
    Message,
    FileMessage,
    FileReplicationMessage,
    FileVersionMessage,
    LSMessage,
    MembershipListMessage,
    MessageType,
)

from Node.nodetypes import (
    bcolors,
    BUFF_SIZE,
)


def in_red(text):
    return bcolors.FAIL + text + bcolors.ENDC


def in_green(text):
    return bcolors.OKGREEN + text + bcolors.ENDC


def in_blue(text):
    return bcolors.OKBLUE + text + bcolors.ENDC


def add_len_prefix(message: bytes) -> bytes:
    msg = struct.pack(">I", len(message)) + message
    return msg


def trim_len_prefix(message: bytes) -> Tuple[int, bytes]:
    msg_len = struct.unpack(">I", message[:4])[0]
    msg = message[4 : 4 + msg_len]
    return msg_len, msg


def _send(
    msg: Message, addr: Tuple[str, int], logger: Optional[logging.Logger] = None
) -> bool:
    if logger is None:
        logger = logging.getLogger(__name__)
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex(addr) != 0:
                raise ConnectionError("Could not connect to {}".format(addr))
            msg_bytes = add_len_prefix(msg.serialize())
            s.sendall(msg_bytes)
        return True

    except Exception as e:
        logger.error(f"Error sending message: {e}")
        print(traceback.format_exc())
    finally:
        return False


def _recvall(sock: socket.socket, logger: Optional[logging.Logger] = None) -> bytearray:
    if logger is None:
        logger = logging.getLogger(__name__)
    # use popular method of recvall
    data = bytearray()
    rec = sock.recv(BUFF_SIZE)
    if len(rec) == 0:
        return data
    # remove the length prefix
    try:
        msg_len, msg = trim_len_prefix(rec)
        data.extend(msg)
    except struct.error:
        print(data)
        raise
    # read the rest of the data, if any
    while len(data) < msg_len:
        msg = sock.recv(BUFF_SIZE)
        if not msg:
            break
        data.extend(msg)
    logger.debug(f"Received {len(data)} bytes from {sock.getpeername()}")
    return data


def _send_file_err(sock, member, file_name):
    # send error message
    error_message = FileMessage(
        MessageType.FILE_ERROR,
        member.ip,
        member.port,
        member.timestamp,
        file_name,
        -1,
        b"",
    )
    sock.sendall(add_len_prefix(error_message.serialize()))


def get_any_open_port() -> int:
    """
    Gets a random open port
    :return: a random open port
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("", 0))
        port = sock.getsockname()[1]

    return port


def infer_model(node, model_type, num_batches):
    # Start an inference job on a model
    pass


def set_batch_size(node, model_type, value):
    # There is a batch size that is set only at the
    # start before any jobs are started.
    pass


def get_query_rate(node, model_type):
    # queries/s over the past 10s
    # return (rate, sd)
    pass


def get_queries_processed(node, model_type):
    # Total queries for a job
    try:
        model_type = int(model_type)
        modeltype = ModelType(model_type)
    except ValueError:
        print("Invalid model type")
        return

    print(f"Queries processed for {modeltype.name}: {node.queries_processed[model_type]}")
    return node.load_balancer.query_counts_by_model[model_type]


def get_vm_job_mapping(node) -> dict:
    # Return a dict of str(VMID) : str(model_type) value
    if not node.is_introducer:
        return

    vm_job_mapping = node.load_balancer.get_vm_job_mapping()

    res = ""
    for vm_id, model_type in vm_job_mapping.items():
        res += f"VMID: {vm_id}, Model Type: {model_type}\n"

    print(res)


def command_C1(node, model_type):
    # Current (over the past 10 seconds) query rate
    # Running count of queries since the beginning

    try:
        model_type = int(model_type)
        model_type = ModelType(model_type)
    except ValueError:
        print("Invalid model type")
        return

    rate = node.load_balancer.get_query_rate()
    processed = node.load_balancer.total_query_count

    print(
        f"Query rate for {model_type.name}: {rate}. Total queries processed: {processed}"
    )


def command_C2(node, model_type):
    # Average and standard deviation of processing times
    # Also consider doing percentiles. This shouldn't be too bad since
    # we will sort the rates, and our data set for processing times
    # shouldn't be too big?
    if not node.is_introducer:
        return
    try:
        modeltype = ModelType(int(model_type[0]))
    except Exception as e:
        print("Invalid model type")
        return

    print(
        f"Average processing time for {modeltype.name}: {node.load_balancer.get_query_rate_model(modeltype)}"
    )


def command_C3(node, model_type, value):
    # Set batch size of model for all nodes
    print("NOT IMPLEMENTED. HARDCODED")


def set_batch_size(node, model_type, value):
    pass


def _get_command_option() -> Tuple[int, List[str]]:
    commands = """Input a command:
    1 -> list_mem: list the membership list
    2 -> list_self: list self's id
    3 -> join: join the group
    4 -> leave: leave the group
    5 -> ls all: list files in the filestore
    6 -> ls <file>: list specific files in filestore
    7 -> store: List the files currently stored in this node's filestore
    ======ML COMMANDS (COORDINATOR ONLY)=========
    MODEL TYPES: 1=RESNET, 2=ALEXNET
    8 -> C1 <model type>: Current (over the past 10 seconds) query rate
    9 -> C2: Average and standard deviation of processing times
    10 -> C3: Set batch size of model for all nodes
    """
    # unindent the commands using textwrap
    commands = textwrap.dedent(commands)

    command = input(commands).split()

    try:
        command_num = int(command[0])

    except ValueError:
        command_num = -1
    except IndexError:
        command_num = -1

    if len(command) > 1:
        command_args = command[1:]
    else:
        command_args = []

    return command_num, command_args


def _handle_command(node, command):
    # the first number in the command is the command number
    start = time.time()
    if len(command) == 0:
        print("Invalid command")
        return
    command_num = command[0]
    if command_num == 1:
        membership_list = node.get_membership_list()
        print(bcolors.OKBLUE + str(membership_list) + bcolors.ENDC)
    elif command_num == 2:
        print(bcolors.OKBLUE + "NODE ID: " + str(node.get_self_id()) + bcolors.ENDC)
    elif command_num == 3:
        node.join_network()
    elif command_num == 4:
        node.leave_network()
    elif command_num == 5:
        node.send_ls()
    elif command_num == 6:
        # command is the array, so the file name is the second element
        # command[1] is a list of commands, so we want the first element
        if len(command) < 2 or len(command[1]) == 0 or command[1][0] == "":
            print("Missing file name")
            return
        node.send_ls(command[1])

    elif command == 7:
        file_store = node.get_file_store()
        filenames = file_store.get_file_names()
        print(bcolors.OKBLUE + ",".join(filenames) + bcolors.ENDC)

    elif command_num == 8:
        # C1 <model type>: Current (over the past 10 seconds) query rate
        if len(command) < 1 or len(command[1]) == 0:
            print("Missing model type")
            return

        print(command)
        command_C1(node, command[1][0])

    elif command_num == 9:
        # C2: Average and standard deviation of processing times
        if len(command) < 1:
            print("Missing model type")
            return
        command_C2(node, command[1])

    elif command_num == 10:
        # C3: Set batch size of model for all nodes
        if len(command) < 1:
            print("Missing model type or batch size")
            return
        command_C3(node, command[1], command[2])

    else:
        print("Invalid command")
    end = time.time()
    print(f"Command took {end - start} seconds")


def run_node_command_menu(node):
    while True:
        command_opts = _get_command_option()
        _handle_command(node, command_opts)


def timed_out(timestamp, timeout):
    return time.time() - timestamp > timeout


def is_membership_message(message_type: int) -> bool:
    return message_type == MessageType.MEMBERSHIP_LIST


def is_communication_message(message_type: int) -> bool:
    """
    Checks if the message is a communication message
    :param message_type: the message type
    :return: True if the message is a communication message, False otherwise
    """
    return (
        message_type == MessageType.NEW_NODE
        or message_type == MessageType.JOIN
        or message_type == MessageType.LEAVE
        or message_type == MessageType.PING
        or message_type == MessageType.PONG
    )


def is_election_message(message_type: int) -> bool:
    """
    Checks if the message is an election message
    :param message_type: the message type
    :return: True if the message is an election message, False otherwise
    """
    # future work
    return (
        message_type == MessageType.ELECT_PING
        or message_type == MessageType.CLAIM_LEADER_PING
        or message_type == MessageType.CLAIM_LEADER_ACK
    )


def is_filestore_message(message_type: int) -> bool:
    """
    Checks if the message is a filestore message
    :param message_type: the message type
    :return: True if the message is a filestore message, False otherwise
    """
    return (
        message_type == MessageType.PUT
        or message_type == MessageType.GET
        or message_type == MessageType.DELETE
        or message_type == MessageType.FILE_ACK
        or message_type == MessageType.FILE_ERROR
    )


def is_ls_message(message_type: int) -> bool:
    """
    Checks if the message is a ls message
    :param message_type: the message type
    :return: True if the message is a ls message, False otherwise
    """
    return message_type == MessageType.LS


def is_file_replication_message(message_type: int) -> bool:
    """
    Checks if the message is a file replication message
    :param message_type: the message type
    :return: True if the message is a file replication message, False otherwise
    """
    return (
        message_type == MessageType.FILE_REPLICATION_REQUEST
        or message_type == MessageType.FILE_REPLICATION_ACK
    )


def is_fileversion_message(message_type: int) -> bool:
    return message_type == MessageType.GET_VERSIONS or message_type == MessageType.GET


# def is_ml_message(message_type: int) -> bool:


def is_ml_schedule_batch_message(message_type: int) -> bool:
    return message_type == MessageType.SCHEDULE_BATCH


def is_ml_batch_size_message(message_type: int) -> bool:
    return message_type == MessageType.SET_BATCH_SIZE


def is_ml_batch_result_message(message_type: int) -> bool:
    return (
        message_type == MessageType.BATCH_COMPLETE
        or message_type == MessageType.BATCH_FAILED
    )


def is_ml_client_request_message(message_type: int) -> bool:
    return message_type == MessageType.CLIENT_INFERERNCE_REQUEST


def is_ml_client_response_message(message_type: int) -> bool:
    return message_type == MessageType.CLIENT_INFERERNCE_RESPONSE


def get_message_from_bytes(data: Union[bytes, bytearray]) -> Message:
    """
    Factory method to get either a Message, FileStoreMessage, or ElectionMessage
    from a byte array.

    :param data: the bytes received
    :return: the Message
    """

    if len(data) == 0:
        return

    # the first byte of the message is the message type
    # get it with struct.unpack

    message_type = struct.unpack(">I", data[:4])[0]

    if is_communication_message(message_type):
        return Message.deserialize(data)

    elif is_filestore_message(message_type):
        return FileMessage.deserialize(data)

    elif is_fileversion_message(message_type):
        return FileVersionMessage.deserialize(data)

    elif is_ls_message(message_type):
        return LSMessage.deserialize(data)

    elif is_membership_message(message_type):
        return MembershipListMessage.deserialize(data)

    elif is_election_message(message_type):
        raise NotImplementedError

    elif is_file_replication_message(message_type):
        return FileReplicationMessage.deserialize(data)

    elif is_ml_schedule_batch_message(message_type):
        return MLBatchScheduleMessage.deserialize(data)

    elif is_ml_batch_size_message(message_type):
        return MLBatchSizeMessage.deserialize(data)

    elif is_ml_batch_result_message(message_type):
        return MLBatchResultMessage.deserialize(data)

    elif is_ml_client_request_message(message_type):
        return MLClientInferenceRequest.deserialize(data)

    elif is_ml_client_response_message(message_type):
        return MLClientInferenceResponse.deserialize(data)

    else:
        raise ValueError(f"Invalid message type {message_type}")


# Useful for displaying/debugging purposes, not used for functionality
# ip_url_dict = {
#     socket.gethostbyname(
#         f"fa22-cs425-25{i:02}.cs.illinois.edu"
#     ): f"fa22-cs425-25{i:02}.cs.illinois.edu"
#     for i in range(1, 10)
# }


def get_replication_level(num_nodes, replication_factor):
    """
    Gets the replication level for the filestore
    :param num_nodes: the number of nodes in the membership list
    :param replication_factor: the replication factor
    :return: the replication level
    """
    return min(num_nodes, replication_factor)
