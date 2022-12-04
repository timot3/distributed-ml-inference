import random
import time
from enum import IntEnum
import os
from typing import List, Optional, Tuple
import json
import requests
import numpy as np
from PIL import Image
from queue import Queue
from threading import Lock, Thread

from ML.utils import load_learner
from Node.messages import (
    Message,
    MessageType,
    FileMessage,
)
from Node.node import NodeTCPServer


class ToDoException(Exception):
    """TODO remove this class"""

    pass


class ModelType(IntEnum):
    RESNET = 0
    IMAGENET = 1
    UNSPECIFIED = 2


QUEUE_CAPACITY = 64  # Arbitrary


class MLModel:
    def __init__(self):
        self.model = None
        # Hyperparameter to be set.
        # Utility comes from using this after coordinator failure
        self.batch_size = 1

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

    def _load(self):
        raise NotImplementedError

    def train(self):
        raise NotImplementedError

    def set_batch_size(self, val: int):
        self.batch_size = val


class ClassifierModel(MLModel):
    def __init__(self, model_type: ModelType):
        super().__init__()
        self.model_type = model_type

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
                    model_url = config["ImageNet"]["model_url"]
            model_pkl_file = requests.get(model_url).content
            with open(model_pkl_path, "wb") as pkl_file:
                pkl_file.write(model_pkl_file)

        self.model = load_learner(model_pkl_path)

    def train(self):
        self._load()


class DummyModel(MLModel):
    def __init__(self):
        super().__init__()

    def _load(self, model_pkl_path: str):
        pass

    def train(self):
        pass


class ModelCollection:
    def __init__(self, server: NodeTCPServer) -> None:
        self.server = server
        self.resnet = ClassifierModel(ModelType.RESNET)
        self.imagenet = ClassifierModel(ModelType.IMAGENET)
        self.workDistThread = Thread()
        self.batch_lock = Lock()
        self.current_batch_id = None
        self.current_image_list = None
        self.current_model_type = None

        """
        self.probScaleThread = Thread()
        # Dynamically adjusted based on query rates
        # Assumption: Infinite work to do, so we will not need to account
        # for the case where 1 queue is empty in the steady state.
        # Therefore, we can just use local query rates of the 2 models to
        # control this probability.
        self.pick_resnet_prob = 0.5
        """

    """
    # To be run by ProbScaleThread
    def update_formula(self):
        # Scaling factor k on probability will equalize the query rate
        # assuming rate is directly proportional to probability of issue
        # We will not immediate scale by this factor to prevent oscillations
        # Assumption of steady state implies the model should generally not
        # deal with cases of 0 query rate.
        query_rate_resnet = self.resnet.get_query_rate()
        query_rate_imagenet = self.imagenet.get_query_rate()
        # Implicit assumption that query rate is much more significant
        # than 0.001
        query_rate_resnet += 0.001  # Prevent 0 division errors
        query_rate_imagenet += 0.001  # Prevent 0 division errors
        k_denom = (
            1 - self.pick_resnet_prob
        ) * query_rate_resnet + query_rate_imagenet * self.pick_resnet_prob
        k_numerator = query_rate_resnet
        k = k_numerator / k_denom
        # Prevent oscillation, reduce weightage of scaling factor k
        self.pick_resnet_prob *= 0.5 * k

    # To be run by WorkDistThread
    def work_dist(self):
        # Assumption: High batch dispatch rate if there is empty queue.
        # Pick a random float from 0 to 1.
        rand_sample = random.uniform(0, 1)
        # If float is smaller than self.pick_resnet_prob, choose resnet.
        # Otherwise, choose lenet
        if rand_sample < self.pick_resnet_prob:
            chosen_model = self.resnet
        else:
            chosen_model = self.imagenet

        chosen_model.infer_once()
        if chosen_model.check_batch_successful():
            # Create thread to write to SDFS
            # Enqueue
            # Even though infer once and enqueue images access the lock twice
            # (non-atomic), enqueue images will check if the queue is filled
            # up if there was a batch enqueue, so there will not be lost data.
            chosen_model.enqueue_images()
    """

    def select_model(self, model: ModelType):
        if model == ModelType.RESNET:
            return self.resnet
        else:
            return self.imagenet

    def infer(self):
        with self.batch_lock:
            assert self.current_batch is not None
            self.model = self.select_model(self.current_model_type)
            pred = self.model.predict(self.current_batch)
            # TODO: Store into SDFS
            # Create a message of the prediction and send it back.
            self.successful_batch(pred)

    def insert_batch(self, model_type, batch_id, file_list):
        image_list = []
        for f in file_list:
            # Load from SDFS
            msg = FileMessage(
                MessageType.GET,
                self.server.host,
                self.server.port,
                self.server.timestamp,
                f,
                0,
                b"",
            )
            introducer_member = self.server.membership_list[0]
            # Currently, read or send get to coordinator
            # TODO: Account for failure
            img = self.server.broadcast_to(msg, [introducer_member], recv=True)
            file_list.append(img)

        with self.batch_lock:
            self.current_image_list = image_list
            self.current_batch_id = batch_id
            self.current_model_type = model_type

    def successful_batch(self, predictions):
        raise NotImplementedError

        # TODO: Write this batch's results into SDFS
        # Write self.output_predictions to SDFS

        # Write successful, inform dispatcher that batch is done.
        # Also inform dispatcher the file name written into SDFS
        # using the same message
        # TODO: Change the Message to one with the filename
        msg = Message(
            MessageType.BATCH_COMPLETE,
            self.server.host,
            self.server.port,
            self.server.timestamp,
        )
        # TODO: Inform scheduler
        introducer_member = self.server.membership_list[0]
        self.server.broadcast_to(msg, [introducer_member], recv=True)