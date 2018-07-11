import os
import glob
import logging
import subprocess

from abc import ABCMeta, abstractmethod


class FileSystem(object):
    """ The required interface for any filesystem implementation

    See GCSFileSystem for a full implementation. This FileSystem is intended
    to be extendable to support cloud file systems, encryption strategies, etc...
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def local(self, path):
        """ Check if the path is available as a local file
        """
        pass

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
    def copy(self, sources, dest):
        """ Copy the files in sources (recursively) to dest

        Parameters
        ----------
        sources : list of str
            The list of paths to copy, which can be directories
        dest : str
            The destination for the copy of source(s)
        """
        pass


class GCSFileSystem(FileSystem):
    """ File system interface that supports both local and GCS files
    """
    GCS = 'gs://'

    def __init__(self, parallel=True, quiet=True):
        flags = []
        if parallel:
            flags.append('-m')
        if quiet:
            flags.append('-q')
        self.gcscp = ['gsutil'] + flags + ['cp', '-r']

    def local(self, path):
        """ Check if the path is available as a local file
        """
        return not path.startswith(self.GCS)

    def ls(self, path):
        """ List files correspond to path, including glob wildcards

        Parameters
        ----------
        path : str
            The path to the file or directory to list; supports wildcards
        """
        logging.info('Globbing file content in {}'.format(path))
        if not self.local(path):
            # TODO perhaps use a python API rather than subprocess and gsutil
            with open(os.devnull, 'w') as DEVNULL:
                p = subprocess.Popen(
                    ['gsutil', 'ls', path],
                    stdout=subprocess.PIPE,
                    stderr=DEVNULL,
                )
                stdout = p.communicate()[0]
            output = [line for line in stdout.split('\n') if line and line[-1] != ':']
        elif '**' in path:
            # Manual recursive glob, since in 2.X glob doesn't have recursive support
            path = path.rstrip('*')
            output = []
            for root, subdirs, files in os.walk(path):
                for fname in files:
                    output.append(os.path.join(root, fname))
        elif os.path.isdir(path):
            output = [os.path.join(path, f) for f in os.listdir(path)]
        else:
            output = glob.glob(path)
        return sorted(output)

    def copy(self, sources, dest):
        """ Copy the files in sources (recursively) to dest

        Parameters
        ----------
        sources : list of str
            The list of paths to copy, which can be directories
        dest : str
            The destination for the copy of source(s)
        """
        summary = ', '.join(sources)
        logging.info('Copying {} to {}...'.format(summary, dest))

        # TODO perhaps use a python API rather than subprocess
        if any(self.GCS in x for x in sources + [dest]):
            # at least one location is on GCS
            cmd = self.gcscp
        else:
            cmd = ['cp', '-r']
        subprocess.check_call(cmd + sources + [dest])
