import argparse
import random
import socket
import sys
import os
import threading

from ML.messages import MLBatchResultMessage, MLClientInferenceRequest
from ML.modeltypes import ModelType
from Node.messages import FileMessage, MessageType
from Node.nodetypes import VM1_URL
from Node.utils import _recvall, get_message_from_bytes
from run_filestore_client import send_message, make_file_message


def make_ml_classification_message(file_names, host, port):

    return MLClientInferenceRequest(host, port, 0, ModelType.UNSPECIFIED, file_names)


def infer_model(node, model_type, num_batches):
    # Start an inference job on a model
    pass


def set_batch_size(model_type, value):
    # There is a batch size that is set only at the
    # start before any jobs are started.
    pass


def get_query_rate_sd(node, model_type):
    # queries/s over the past 10s
    # return (rate, sd)
    pass


def get_queries_processed(node, model_type):
    # Total queries for a job
    pass


def get_vm_job_mapping(node) -> dict:
    # Return a dict of str(VMID) : str(model_type) value
    pass


def command_C1(node, model_type):
    # Current (over the past 10 seconds) query rate
    # Running count of queries since the beginning
    rate, sd = get_query_rate_sd(node)
    processed = get_queries_processed(node)
    print("Query rate: ", rate)
    print("Query rate standard deviation: ", sd)
    print("Queries processed: ", processed)


def command_C2(model_type):
    # Average and standard deviation of processing times
    # Also consider doing percentiles. This shouldn't be too bad since
    # we will sort the rates, and our data set for processing times
    # shouldn't be too big?
    pass


def command_C3(model_type, value):
    # Set batch size of model for all nodes
    set_batch_size(model_type, value)
    pass


"""
This command exists to display the results of inference.
Right now, we do not need it since we print our results
out to terminal.
def command_C4():
    pass

"""


def command_C5():
    # This prints out the VM -> job mapping.
    # We can do a job -> VM mapping and then swap the relationship
    # between key and value
    vm_job_mapping = get_vm_job_mapping()
    job_vm_mapping = {v: k for k, v in vm_job_mapping}
    print(job_vm_mapping)


def run_ml_client_server():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("localhost", 8082))
        s.listen()
        print("Listening on port", s.getsockname()[1])
        while True:
            conn, addr = s.accept()
            with conn:
                while True:
                    data = _recvall(conn)
                    if not data:
                        break
                    # parse the data
                    message = MLBatchResultMessage.deserialize(data)
                    print(f"Prediction: {message.results}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--local", action="store_true", help="Run the client locally")
    parser.add_argument("--menu", action="store_true", help="Run the client in menu mode")
    parser.add_argument(
        "--num-files", type=int, default=10, help="Number of files to test"
    )
    parser.add_argument(
        "--num-batches", type=int, default=100, help="Number of batches to test"
    )
    parser.add_argument(
        "--batch-size", type=int, default=4, help="Number of files per batch"
    )

    args = parser.parse_args()
    # if args.local:
    if args.local:
        HOST, PORT = "127.0.0.1", 8080
        BACKUP_HOST, BACKUP_PORT = "127.0.0.1", 8081
    else:
        leader_ip = socket.gethostbyname(VM1_URL)
        HOST, PORT = leader_ip, 8080
        BACKUP_HOST, BACKUP_PORT = leader_ip, 8081

    DIRECTORY_PREFIX = "ML/datasets/oxford_pets/"

    # start a thread running the server
    client_server = threading.Thread(target=run_ml_client_server)
    client_server.daemon = False
    client_server.start()

    # load the first 10 files from ML/datasets/oxford_pets into sdfs
    first_10_files = random.sample(os.listdir(DIRECTORY_PREFIX), args.num_files)
    for file_name in first_10_files:
        if file_name.endswith(".jpg"):
            # load the file into sdfs
            with open(DIRECTORY_PREFIX + file_name, "rb") as f:
                file_data = f.read()
                file_message = make_file_message(
                    file_data, file_name, MessageType.PUT, HOST, PORT
                )
                try:
                    response = send_message(HOST, PORT, file_message)
                except ConnectionRefusedError:
                    response = send_message(BACKUP_HOST, BACKUP_PORT, file_message)
    # inference

    # let's do inference on the first file in the directory1

    for i in range(args.num_batches):
        file_names = random.sample(first_10_files, args.batch_size)
        print(file_names)
        ml_message = make_ml_classification_message(file_names, HOST, PORT)
        try:
            response = send_message(HOST, PORT, ml_message, recv=False)
        except ConnectionRefusedError:
            response = send_message(BACKUP_HOST, BACKUP_PORT, ml_message, recv=False)

    # wait for the server to finish
    client_server.join()
