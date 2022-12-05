import random
import time
from enum import IntEnum
import os
from typing import List, Optional, Tuple, TYPE_CHECKING
import json
import requests
import numpy as np
from PIL import Image
from queue import Queue
from threading import Lock, Thread

from fastai.vision.all import *


from ML.messages import get_file_get_msg, get_batch_complete_msg
from ML.modeltypes import ModelType
from Node.LoadBalancer.Batch import Batch

if TYPE_CHECKING:
    from Node.node import NodeTCPServer


class ToDoException(Exception):
    """TODO remove this class"""

    pass


class MLModel:
    def __init__(self):
        self.model = None
        # Hyperparameter to be set.
        # Utility comes from using this after coordinator failure
        self.batch_size = 8

    """
    Remove this, the dataset should be filled in via SDFS.
    def _download_dataset(self):
        #Download the dataset for the ML model.
        if self.model_dataset == DatasetType.CIFAR10:
            # download the CIFAR10 dataset
            raise ToDoException

        elif self.model_dataset == DatasetType.OXFORD_PETS:
            # download the Oxford Pets dataset
            raise ToDoException
    """

    def _load(self, model_pkl_path: str):
        raise NotImplementedError

    def train(self):
        raise NotImplementedError

    def set_batch_size(self, val: int):
        self.batch_size = val


class ClassifierModel(MLModel):
    def __init__(self, model_type: ModelType):
        super().__init__()
        self.model_type = model_type
        self.model = self.train()
        print(self.model)

    def _load(self, model_pkl_path: str):
        # load the model from the pkl file into a fastai vision_learner
        # first, check if there is a pkl file at model_pkl_path
        # if there is, load the model from the pkl file
        # if there is not, download the model from the dropbox link in config.json
        # and then load the model from the pkl file
        if not os.path.exists(model_pkl_path):
            # get the url from config.json ""
            with open("ML/config.json", "rb") as config_file:
                config = json.load(config_file)
                if self.model_type == ModelType.RESNET:
                    model_url = config["ResNet"]["model_url"]
                else:
                    model_url = config["AlexNet"]["model_url"]
            model_pkl_file = requests.get(model_url).content
            with open(model_pkl_path, "wb") as pkl_file:
                pkl_file.write(model_pkl_file)

        return load_learner(model_pkl_path)

    def train(self):
        if self.model_type == ModelType.RESNET:
            return self._load("ML/models/resnet.pkl")
        else:
            return self._load("ML/models/alexnet.pkl")

    def predict(self, img):
        return self.model.predict(img)


class ModelCollection:
    def __init__(self, server: "NodeTCPServer") -> None:
        self.server = server
        self.resnet = ClassifierModel(ModelType.RESNET)
        self.alexnet = ClassifierModel(ModelType.ALEXNET)
        self.server.logger.info("Loading models...")
        self.resnet.train()
        self.server.logger.info("ResNet loaded")
        # self.alexnet.train()
        self.server.logger.info("ImageNet loaded")

        self.workDistThread = Thread()
        self.batch_lock = Lock()
        self.current_image_list = None
        self.current_batch = None

    def select_model(self, model: ModelType):
        if model == ModelType.UNSPECIFIED:
            # pick resnet with 70% probability
            if random.random() < 0.7:
                return self.resnet
            else:
                return self.alexnet
        elif model == ModelType.RESNET:
            return self.resnet
        else:
            return self.alexnet

    def get_batch_size(self, model: ModelType):
        model = self.select_model(model)
        return model.batch_size

    def infer(self):
        start = time.time()
        assert self.current_batch is not None
        model = self.select_model(self.current_batch.model_type)
        pred = []
        for img in self.current_image_list:
            pred.append(model.predict(img))
            # print(f"Predicted: {pred[-1]}")
        self.successful_batch(pred)
        end = time.time()
        # print(f"Time to infer: {end - start}")

    def insert_batch(self, model_type, batch_id, file_list):
        successful_acquire = self.batch_lock.acquire(False)
        if not successful_acquire:
            # Send NACK to sender
            if self.current_batch is not None:
                msg = get_batch_complete_msg(
                    self.server,
                    self.current_batch.model_type,
                    self.current_batch.id,
                    self.current_batch.files,
                    None,
                )
            else:
                msg = get_batch_complete_msg(
                    self.server, model_type, batch_id, file_list, None
                )
                introducer_member = self.server.membership_list[0]
                self.server.broadcast_to(msg, [introducer_member])
                return

        image_list = []
        # print(f"FILE_LIST: {file_list}")
        for f in file_list:
            # Load from SDFS
            msg = get_file_get_msg(self.server, f)
            introducer_member = self.server.membership_list[0]
            # Currently, read or send get to coordinator
            # TODO: Account for failure
            received_data = self.server.broadcast_to(msg, [introducer_member], recv=True)
            # save the image to a temp file

            img_fastai = PILImage.create(received_data[introducer_member].data)
            image_list.append(img_fastai)

        # with self.batch_lock:
        self.current_batch = Batch(model_type, file_list)
        self.current_image_list = image_list
        self.infer()

        self.batch_lock.release()

    def successful_batch(self, predictions):

        # TODO: Write this batch's results into SDFS
        # Write self.output_predictions to SDFS

        # Write successful, inform dispatcher that batch is done.
        # Also inform dispatcher the file name written into SDFS
        # using the same message
        # TODO: Change the Message to one with the filename
        results = [x[0] for x in predictions]  # we only want the class name
        msg = get_batch_complete_msg(
            self.server,
            self.current_batch.model_type,
            self.current_batch.id,
            self.current_batch.files,
            results,
        )
        # TODO: Inform scheduler
        introducer_member = self.server.membership_list[0]
        self.server.broadcast_to(msg, [introducer_member])
        # query_count_msg =


class DummyModelCollection:
    """Class to simulate a model collection"""

    def __init__(self, server: "NodeTCPServer") -> None:
        self.server = server
        self.dummy = DummyModel()
        self.server.logger.info("Loading models...")
        self.dummy.train()
        self.server.logger.info("Dummy loaded")

    def infer(self, batch_id, file_list):
        time.sleep(1)
        self.successful_batch(file_list)

    def insert_batch(self, request, model_type, batch_id, file_list):
        return self.infer(batch_id, file_list)

    def successful_batch(self, predictions):
        # contact the coordinator with results of the batch
        msg = get_batch_complete_msg(self.server)
