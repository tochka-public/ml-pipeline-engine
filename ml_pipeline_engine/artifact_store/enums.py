from enum import Enum


class DataFormat(str, Enum):
    PICKLE = 'pickle'
    JSON = 'json'
