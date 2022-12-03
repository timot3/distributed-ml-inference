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


class ToDoException(Exception):
    """TODO remove this class"""

    pass


class DatasetType(IntEnum):
    CIFAR10 = 0
    OXFORD_PETS = 1


QUEUE_CAPACITY = 64 # Arbitrary

class MLModel:
    def __init__(self, model_dataset: DatasetType):
        self.model_dataset = model_dataset
        self.model = None
        self.batch_size = 1 
        # self._download_dataset()

        # We let the queue be a queue of lists of images.
        # We do not want insertion to block, so we manually track queue depth rather 
        # than instantiating it with one.
        self.batch_queue = Queue()
        # This accomodates having batch size be != 1, and have the design 
        # infer 1 picture at once - less efficient than inferring entire 
        # batch but much better load balance
        self.image_fname_queue = Queue()
        # Only useful for defensive programming
        # Restrict this variable to inside infer_batch only if not
        # coding asserts
        self.cur_batch_size = self.batch_size
        self.predictions_lock = Lock()
        self.output_predictions = []
        self.queries = 0 # Includes those not committed into FS
        self.complete_queries = 0

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

    # TODO: Corner case - FIFO is filled up. Handle outside function or inside?
    def enqueue_batch(self, file_names: List):
        # Spawn thread to collect relevant information (image(s))
        # Thread should just get the file(s) from sdfs
        image_list = []
        for f in file_names:
            # TODO: implement get from SDFS
            image = None
            image_list.append((image, f))
            pass
        # Add the image(s) to the queue
        self.batch_queue.put(image_list)
        self.enqueue_images()

    # This will be called after an enqueue batch and after inferring 
    # an entire batch.
    def enqueue_images(self):
        # Make sure batch queue has entries
        # Guarantees that the predictions are updated properly too.
        with self.predictions_lock:
            if self.batch_queue.empty():
                return
            else: 
                batch = self.batch_queue.get()
            # Slight overengineering to account for possibility that the batch size
            # is changed, even though it shouldn't be in the demo.
            # Otherwise we can use self.batch_size
            self.cur_batch_size = len(batch)
            for x in batch:
                self.image_fname_queue.put(x)
            self.output_predictions = []

    def check_batch_successful(self):
        with self.predictions_lock:
            return len(self.output_predictions) == self.cur_batch_size
                
    # Use a thread to run this
    def successful_batch(self):
        
        # TODO: Write this batch's results into SDFS
        # Write self.output_predictions to SDFS
        pass

        # Write successful, inform dispatcher that batch is done.
        # Also inform dispatcher the file name written into SDFS
        # using the same message
        # TODO: Inform dispatcher

        # TODO: Account for coordinator failures/change of leader etc. This should be done in a future phase, not now.
        self.complete_queries += len(self.output_predictions)



    def infer_once(self):
        # Empty queue, wait for it to fill up
        if self.image_fname_queue.empty():
            time.sleep(0.001)
        
        else: 
            img_fname = self.image_fname_queue.get()
            self.infer_single_image(img_fname)

        self.queries += 1 



    # Image should be a single image, we do NOT want batch prediction
    # Easier to load balance
    def infer_single_image(self, img_fname):
        image, file_name = img_fname
        pred = self.model.predict(image)
        with self.predictions_lock:
            self.output_predictions.append(self.model.predict(image))
        return 


    def set_batch_size(self, val: int):
        self.batch_size = val


class ClassifierModel(MLModel):
    def __init__(self, model_dataset: DatasetType):
        super().__init__(model_dataset)
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


class DummyModel(MLModel):
    def __init__(self, model_dataset: DatasetType):
        super().__init__(model_dataset)

    def _load(self, model_pkl_path: str):
        pass

    def train(self):
        pass

    
class ModelCollection:
    def __init__(self) -> None:
        self.resnet = ClassifierModel()
        self.lenet = ClassifierModel()
        self.workDistThread = Thread()
        self.probScaleThread = Thread()
        # Dynamically adjusted based on query rates
        # Assumption: Infinite work to do, so we will not need to account
        # for the case where 1 queue is empty in the steady state.
        # Therefore, we can just use local query rates of the 2 models to
        # control this probability.
        self.pick_resnet_prob = 0.5
    
    # To be run by ProbScaleThread
    def update_formula(self, query_rate_resnet, query_rate_lenet):
        # Scaling factor k on probability will equalize the query rate
        # We will not immediate scale by this factor to prevent oscillations
        # Assumption of steady state implies the model should generally not 
        # deal with cases of 0 query rate.
        # Implicit assumption that query rate is much more significant 
        # than 0.001
        query_rate_resnet += 0.001 # Prevent 0 division errors
        query_rate_lenet += 0.001 # Prevent 0 division errors
        k_denom = (1 - self.pick_resnet_prob) * query_rate_resnet + query_rate_lenet * self.pick_resnet_prob
        k_numerator = query_rate_lenet
        k = k_numerator / k_denom
        # Prevent oscillation, reduce weightage of scaling factor k
        self.pick_resnet_prob *= 0.5 * k

    # To be run by WorkDistThread
    def work_dist(self):
        # Assumption: High batch dispatch rate if there is empty queue.
        # Pick a random float from 0 to 1.
        # If float is smaller than self.pick_resnet_prob, choose resnet.
        # Otherwise, choose lenet 
        if True:
            chosen_model = self.resnet
        else:
            chosen_model = self.lenet
        
        chosen_model.infer_once()
        if chosen_model.check_batch_successful():
            # Create thread to write to SDFS
            # Enqueue 
            # Even though infer once and enqueue images access the lock twice 
            # (non-atomic), enqueue images will check if the queue is filled 
            # up if there was a batch enqueue, so there will not be lost data.
            chosen_model.enqueue_images()

