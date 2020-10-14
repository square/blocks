from abc import ABC, abstractmethod

from typing import Union, IO, Sequence, Iterator, List, Any, Generator, ContextManager
from blocks.datafile import DataFile
from contextlib import contextmanager


class FileSystem(ABC):
    """The required interface for any filesystem implementation

    See GCSFileSystem for a full implementation. This FileSystem is intended
    to be extendable to support cloud file systems, encryption strategies, etc...
    """

    @abstractmethod
    def ls(self, path: str) -> Sequence[str]:
        """List files correspond to path, including glob wildcards

        Parameters
        ----------
        path : str
            The path to the file or directory to list; supports wildcards
        """
        pass

    @abstractmethod
    def access(self, paths: Sequence[str]) -> List[DataFile]:
        """Access multiple paths as file-like objects

        This allows for optimization like parallel downloads

        Parameters
        ----------
        paths: list of str
            The paths of the files to access

        Returns
        -------
        files: list of DataFile
            A list of datafile instances, one for each input path
        """
        pass

    @abstractmethod
    def store(self, bucket: str, files: Sequence[str]) -> ContextManager:
        """Store multiple data objects

        This allows for optimizations when storing several files

        Parameters
        ----------
        bucket : str
            The GCS bucket to use to store the files
        files : list of str
            The file names to store

        Returns
        -------
        datafiles : contextmanager
           A contextmanager that will yield datafiles and place them
           on the filesystem when finished
        """
        pass

    @abstractmethod
    @contextmanager
    def open(self, path, mode="rb"):
        """Access path as a file-like object

        Parameters
        ----------
        path: str
            The path of the file to access
        mode: str
            The file mode for the opened file

        Returns
        -------
        file: file
            A python file opened to the provided path (uses a local temporary copy that is removed)
        """
        pass
