import os
from abc import ABC, abstractmethod
from contextlib import contextmanager

from typing import IO, Generator


class DataFile(ABC):
    """A datafile that holds the source path and method to get a handle"""

    def __init__(self, path: str):
        self.path = path

    @abstractmethod
    def handle(self):
        pass


class LocalDataFile(DataFile):
    def __init__(self, path: str, local: str):
        self.local = local
        super(LocalDataFile, self).__init__(path)

    @contextmanager
    def handle(self, mode: str = "rb") -> Generator[IO, None, None]:
        if mode.startswith("w") and not os.path.isdir(os.path.dirname(self.local)):
            os.makedirs(os.path.dirname(self.local))
        with open(self.local, mode) as f:
            yield f
