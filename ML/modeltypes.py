from enum import IntEnum


class ModelType(IntEnum):
    UNSPECIFIED = 0
    RESNET = 1
    ALEXNET = 2


QUEUE_CAPACITY = 64  # Arbitrary
