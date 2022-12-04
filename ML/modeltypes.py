from enum import IntEnum


class ModelType(IntEnum):
    RESNET = 0
    ALEXNET = 1
    UNSPECIFIED = 2


QUEUE_CAPACITY = 64  # Arbitrary
