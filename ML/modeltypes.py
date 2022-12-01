from enum import IntEnum
import os
from typing import List, Optional, Tuple
import json
import requests
import numpy as np
from PIL import Image

from ML.utils import load_learner


class ToDoException(Exception):
    """TODO remove this class"""

    pass


class DatasetType(IntEnum):
    CIFAR10 = 0
    OXFORD_PETS = 1


class MLModelType(IntEnum):
    CLASSIFIER = 0
    OBJECT_DETECTION = 1


class MLModel:
    def __init__(self, model_type: MLModelType, model_dataset: DatasetType):
        self.model_type = model_type
        self.model_dataset = model_dataset
        self.model = None
        self.batch_size = 1 # TODO: Implement a method to update the batch size
        self._download_dataset()

    def _download_dataset(self):
        """Download the dataset for the ML model."""
        if self.model_dataset == DatasetType.CIFAR10:
            # download the CIFAR10 dataset
            raise ToDoException

        elif self.model_dataset == DatasetType.OXFORD_PETS:
            # download the Oxford Pets dataset
            raise ToDoException

    def _load(self, model_pkl_path: str):
        raise NotImplementedError

    def train():
        raise NotImplementedError

    def infer_batch(self, batch_size: int, images):
        # learn.predict
        raise NotImplementedError

    def infer(self, image):
        return self.infer_batch(1, [image])[0]

    def set_batch_size(self, val: int):
        self.batch_size = val


class ClassifierModel(MLModel):
    def __init__(self, model_dataset: DatasetType):
        super().__init__(MLModelType.CLASSIFIER, model_dataset)
        self._download_dataset()

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
            model_url = config["ResNet"]["model_url"]
            model_pkl_file = requests.get(model_url).content
            with open(model_pkl_path, "wb") as pkl_file:
                pkl_file.write(model_pkl_file)

        self.model = load_learner(model_pkl_path)

    def train(self):
        self._load()

    def infer_batch(self, batch_size: int, images):
        raise NotImplementedError

    def infer(self, image: Image):
        return self.infer_batch(1, [image])[0]
