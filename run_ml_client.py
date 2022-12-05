import argparse
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--local", action="store_true", help="Run the client locally")
    parser.add_argument("--menu", action="store_true", help="Run the client in menu mode")
    parser.add_argument(
        "--num-files", type=int, default=10, help="Number of files to test"
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
    first_10_files = os.listdir(DIRECTORY_PREFIX)[: args.num_files]
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
    # let's do inference on the first file in the directory
    file_name = first_10_files[0]
    ml_message = make_ml_classification_message([file_name], HOST, PORT)

    # send the message
    response = send_message(HOST, PORT, ml_message)
