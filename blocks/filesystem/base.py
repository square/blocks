from abc import ABCMeta, abstractmethod

from six import add_metaclass


@add_metaclass(ABCMeta)
class FileSystem(object):
    """ The required interface for any filesystem implementation

    See GCSFileSystem for a full implementation. This FileSystem is intended
    to be extendable to support cloud file systems, encryption strategies, etc...
    """

    @abstractmethod
    def ls(self, path):
        """ List files correspond to path, including glob wildcards

        Parameters
        ----------
        path : str
            The path to the file or directory to list; supports wildcards
        """
        pass

    @abstractmethod
    def access(self, paths):
        """ Access multiple paths as file-like objects

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
    def store(self, bucket, files):
        """ Store multiple data objects

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
