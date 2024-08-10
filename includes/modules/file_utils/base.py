from abc import ABC
from typing import List, Union

class Handler(ABC):
    def __init__(self, path: Union[str, List[str]]):
        if isinstance(path, str):
            self.path = [path]
        elif isinstance(path, list) and all(isinstance(p, str) for p in path):
            self.path = path
        else:
            raise ValueError("path must be a string or a list of strings")
