import fnmatch
import os
import glob
import logging
import subprocess
import tempfile
import atexit
import shutil
import time
import wrapt
import requests

from abc import ABCMeta, abstractmethod
from six import add_metaclass
from io import BytesIO
from collections import namedtuple
from contextlib import contextmanager
from google.cloud import storage

DataFile = namedtuple('DataFile', ['path', 'handle'])


@wrapt.decorator
def _retry_with_backoff(wrapped, instance, args, kwargs):
    trial = 0
    while True:
        wait = 2**(trial+2) # 4s up to 128s
        try:
            return wrapped(*args, **kwargs)
        except requests.exceptions.ConnectionError:
            if trial == 6:
                raise
            logging.info('{} failed to connect, retrying after {}s'.format(wrapped.__name__, wait))
            trial += 1
        time.sleep(wait)


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


class GCSFileSystem(FileSystem):
    """ File system interface that supports both local and GCS files

    This implementation uses subprocess and gsutil, which has excellent performance.
    However this can lead to problems in very multi-threaded applications and might not be
    as portable. For a python native implementation use GCSNativeFileSystem
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
            with open(os.devnull, 'w') as DEVNULL:
                p = subprocess.Popen(
                    ['gsutil', 'ls', path],
                    stdout=subprocess.PIPE,
                    stderr=DEVNULL,
                    universal_newlines=True,
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
        return sorted(p.rstrip('/') for p in output)

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
        # Break this into pieces so we don't hit OS limit for arguments
        # TODO: use getconf ARG_MAX and actually test number of bytes in sources
        CHUNK_SIZE = 1000
        sources_chunks = [sources[x:x+CHUNK_SIZE] for x in range(0, len(sources), CHUNK_SIZE)]
        for sources in sources_chunks:
            subprocess.check_call(cmd + sources + [dest])

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
        # Move the files into a tempdir from GCS
        tmpdir = _session_tempdir()
        self.copy(paths, tmpdir)

        # Then get file handles for each
        datafiles = []
        for path in paths:
            local = os.path.join(tmpdir, os.path.basename(path))
            datafiles.append(DataFile(path, open(local, 'rb')))
        return datafiles

    @contextmanager
    def store(self, bucket, files):
        """ Create file stores that will be written to the filesystem on close

        This allows for optimizations when storing several files

        Parameters
        ----------
        bucket : str
            The path of the bucket (on GCS) or folder (local) to store the data in
        files : list of str
            The filenames to create

        Returns
        -------
        datafiles : contextmanager
            A context manager that yields datafiles and when the context is closed
            they are written to GCS

        Usage
        -----
        >>> with filesystem.store('gs://bucket/sub/', ['ex1.txt', 'ex2.txt']) as datafiles:
        >>>     datafiles[0].handle.write('example 1')
        >>>     datafiles[1].handle.write('example 2')
        """
        # Make local files in a tempdir that serve as the file handles
        tmpdir = _session_tempdir()
        datafiles = []
        local_files = []
        for f in files:
            local = os.path.join(tmpdir, f)
            local_files.append(local)
            datafiles.append(DataFile(os.path.join(bucket, f), open(local, 'wb')))

        yield datafiles

        for d in datafiles:
            d.handle.close()

        if self.local(bucket) and not os.path.exists(bucket):
            os.makedirs(bucket)

        self.copy(local_files, os.path.join(bucket, ''))


class GCSNativeFileSystem(GCSFileSystem):
    """ File system interface that supports GCS and local files

    This uses the native python cloud storage library for read and write, rather than gsutil.
    The performance is significantly slower when reading/writing several files but is thread-safe
    for applications which are already parallelized. It also stores the files entirely in
    memory rather than using tempfiles.
    """
    def __init__(self, *args, **kwargs):
        self.client = storage.Client()
        super(GCSNativeFileSystem, self).__init__(*args, **kwargs)

    def ls(self, path):
        logging.info('Globbing file content in {}'.format(path))

        # use GCSFileSystem's implementation for local paths
        if self.local(path):
            return super(GCSNativeFileSystem, self).ls(path)

        # find all names that start with the prefix for this path
        bucket, path = self._split(path)
        prefix = self._prefix(path)
        names = [b.name for b in self.client.get_bucket(bucket).list_blobs(prefix=prefix)]

        if prefix == path:
            # no pattern strings, listing a single file or a folder
            try:
                # one blob exactly matches the requested path
                names = [next(p for p in names if p == path)]
            except StopIteration:
                # no exact match, so we are listing based on prefix
                # find files in this directory or subfolders
                names = self._list_single(prefix, names)
        # we have a pattern to match
        else:
            # for recursive glob, do not attempt to match folders, only files
            if '**' in path:
                names = fnmatch.filter(names, path)
            else:
                # make sure we can match folders in addition to blobs
                candidates = set(names) | set(self._list_single(prefix, names))
                names = fnmatch.filter(candidates, path)
                # one more filter because fnmatch recurses single *
                names = [name for name in names if name.count('/') == path.count('/')]

        paths = ['gs://{}/{}'.format(bucket, name) for name in names]
        return sorted(paths)

    def _prefix(self, path):
        # find the lowest prefix that doesn't include a pattern match
        splits = path.split('/')
        accumulate = []
        while splits:
            sub = splits.pop(0)
            if any(x in sub for x in ['*', '?', '[', ']']):
                break
            accumulate.append(sub)
        return '/'.join(accumulate) if accumulate else None

    def _list_single(self, prefix, names):
        # find files that are directly in the dir specified by prefix, not in subfolders
        valid = set(n.replace(prefix, '').lstrip('/').split('/')[0] for n in names)
        return [os.path.join(prefix, n) for n in valid]

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
        datafiles = []
        for path in paths:
            datafiles.append(DataFile(path, BytesIO()))
        for datafile in datafiles:
            self._read(datafile)
        return datafiles

    @contextmanager
    def store(self, bucket, files):
        """ Create file stores that will be written to the filesystem on close

        This allows for optimizations when storing several files

        Parameters
        ----------
        bucket : str
            The path of the bucket (on GCS) or folder (local) to store the data in
        files : list of str
            The filenames to create

        Returns
        -------
        datafiles : contextmanager
            A context manager that yields datafiles and when the context is closed
            they are written to GCS

        Usage
        -----
        >>> with filesystem.store('gs://bucket/sub/', ['ex1.txt', 'ex2.txt']) as datafiles:
        >>>     datafiles[0].handle.write('example 1')
        >>>     datafiles[1].handle.write('example 2')
        """
        # Make BytesIO instances that serve as the file handles
        datafiles = []
        for f in files:
            datafiles.append(DataFile(os.path.join(bucket, f), BytesIO()))

        yield datafiles

        for d in datafiles:
            d.handle.seek(0)
            self._write(d)

    def _split(self, path):
        bucket = path.replace(self.GCS, '').split('/')[0]
        prefix = "gs://{}".format(bucket)
        path = path[len(prefix) + 1:]
        return bucket, path

    @_retry_with_backoff
    def _blob(self, path):
        bucket, path = self._split(path)
        return storage.Blob(path, self.client.get_bucket(bucket))

    def _read(self, datafile):
        if self.local(datafile.path):
            with open(datafile.path, 'rb') as f:
                datafile.handle.write(f.read())
        else:
            self._blob(datafile.path).download_to_file(datafile.handle)
        datafile.handle.seek(0)

    def _write(self, datafile):
        if self.local(datafile.path):
            with open(datafile.path, 'w') as f:
                f.write(datafile.handle.read())
        else:
            self._blob(datafile.path).upload_from_file(datafile.handle)


def _session_tempdir():
    """ Create a tempdir that will be cleaned up at session exit
    """
    tmpdir = tempfile.mkdtemp()
    # create and use a subdir of specified name to preserve cgroup logic
    atexit.register(lambda: shutil.rmtree(tmpdir))
    return tmpdir
