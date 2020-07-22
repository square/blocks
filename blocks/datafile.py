import os
from abc import ABCMeta, abstractmethod
from contextlib import contextmanager

from six import add_metaclass


@add_metaclass(ABCMeta)
class DataFile:
    """ A datafile that holds the source path and method to get a handle
    """

    def __init__(self, path):
        self.path = path

    @abstractmethod
    def handle(self):
        pass


class LocalDataFile(DataFile):
    def __init__(self, path, local):
        self.local = local
        super(LocalDataFile, self).__init__(path)

    @contextmanager
    def handle(self, mode="rb"):
        if mode.startswith("w") and not os.path.isdir(os.path.dirname(self.local)):
            os.makedirs(os.path.dirname(self.local))
        with open(self.local, mode) as f:
            yield f
