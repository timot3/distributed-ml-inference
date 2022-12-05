import argparse
import random
import socket
import sys
import os

from ML.messages import MLClientInferenceRequest
from ML.modeltypes import ModelType
from Node.messages import FileMessage, MessageType
from Node.nodetypes import VM1_URL
from run_filestore_client import send_message, make_file_message


def make_ml_classification_message(file_names, host, port):

    return MLClientInferenceRequest(host, port, 0, ModelType.UNSPECIFIED, file_names)

def infer_model(model_type, num_batches):
    # Start an inference job on a model
    pass

def set_batch_size(model_type, value):
    # There is a batch size that is set only at the 
    # start before any jobs are started.
    pass

def get_query_rate_sd(model_type):
    # queries/s over the past 10s
    # return (rate, sd)
    pass

def get_queries_processed(model_type):
    # Total queries for a job
    pass

def get_vm_job_mapping() -> dict:
    # Return a dict of str(VMID) : str(model_type) value
    pass

def command_C1(model_type):
    # Current (over the past 10 seconds) query rate
    # Running count of queries since the beginning
    rate, sd = get_query_rate_sd()
    processed = get_queries_processed()
    pass

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
    job_vm_mapping = {v : k for k, v in vm_job_mapping} 
    print(job_vm_mapping)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--local", action="store_true", help="Run the client locally")
    parser.add_argument("--menu", action="store_true", help="Run the client in menu mode")
    parser.add_argument(
        "--num-files", type=int, default=100, help="Number of files to test"
    )
    parser.add_argument(
        "--num-batches", type=int, default=10, help="Number of batches to test"
    )

    args = parser.parse_args()
    # if args.local:
    if True:
        HOST, PORT = "127.0.0.1", 8080
    else:
        leader_ip = socket.gethostbyname(VM1_URL)
        HOST, PORT = leader_ip, 8080

    DIRECTORY_PREFIX = "ML/datasets/oxford_pets/"

    # load the first 10 files from ML/datasets/oxford_pets into sdfs
    first_10_files = os.listdir(DIRECTORY_PREFIX)
    for file_name in first_10_files:
        if file_name.endswith(".jpg"):
            # load the file into sdfs
            with open(DIRECTORY_PREFIX + file_name, "rb") as f:
                file_data = f.read()
                file_message = make_file_message(
                    file_data, file_name, MessageType.PUT, HOST, PORT
                )
                response = send_message(HOST, PORT, file_message)

    # inference
    # let's do inference on the first file in the directory1

    # for i in range(args.num_batches):
    #     file_names = random.sample(first_10_files, 8)
    #     print(file_names)
    #     ml_message = make_ml_classification_message(file_names, HOST, PORT)
    #     response = send_message(HOST, PORT, ml_message, recv=False)

    start = time.time()
