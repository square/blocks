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
from six import add_metaclass, PY3, string_types
from io import BytesIO, TextIOWrapper
from collections import namedtuple
from contextlib import contextmanager
from google.cloud import storage

DataFile = namedtuple('DataFile', ['path', 'handle'])


@wrapt.decorator
def _retry_with_backoff(wrapped, instance, args, kwargs):
    trial = 0
    while True:
        wait = 2**(trial+2)  # 4s up to 128s
        try:
            return wrapped(*args, **kwargs)
        except (requests.exceptions.ConnectionError, requests.exceptions.ChunkedEncodingError):
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
        self.gcscp = ['gsutil'] + flags + ['cp']

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

    def rm(self, paths, recursive=False):
        """ Remove the files at paths

        Parameters
        ----------
        paths : list of str
            The paths to remove
        recursive : bool, default False
            If true, recursively remove any directories
        """
        if isinstance(paths, string_types):
            paths = [paths]

        if any(not self.local(p) for p in paths):
            # at least one location is on GCS
            cmd = ['gsutil', '-m', 'rm']

        else:
            cmd = ['rm']

        if recursive:
            cmd.append('-r')

        CHUNK_SIZE = 1000
        paths_chunks = [paths[x:x+CHUNK_SIZE] for x in range(0, len(paths), CHUNK_SIZE)]
        for paths in paths_chunks:
            subprocess.check_call(cmd + paths)

    def cp(self, sources, dest, recursive=False):
        """ Copy the files in sources to dest

        Parameters
        ----------
        sources : list of str
            The list of paths to copy
        dest : str
            The destination for the copy of source(s)
        recursive : bool
            If true, recursively copy any directories
        """
        if isinstance(sources, string_types):
            sources = [sources]

        summary = ', '.join(sources)
        logging.info('Copying {} to {}...'.format(summary, dest))

        if any(self.GCS in x for x in sources + [dest]):
            # at least one location is on GCS
            cmd = self.gcscp
        else:
            cmd = ['cp']

        if recursive:
            cmd.append('-r')

        CHUNK_SIZE = 1000
        sources_chunks = [sources[x:x+CHUNK_SIZE] for x in range(0, len(sources), CHUNK_SIZE)]
        for sources in sources_chunks:
            subprocess.check_call(cmd + sources + [dest])

    @contextmanager
    def open(self, path, mode='rb'):
        """ Access path as a file-like object

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
        with tempfile.NamedTemporaryFile() as nf:
            if mode.startswith('r'):
                self.cp(path, nf.name)

            nf.seek(0)

            with open(nf.name, mode) as f:
                yield f

            nf.seek(0)

            if mode.startswith('w'):
                self.cp(nf.name, path)

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
        self.cp(paths, tmpdir, recursive=True)

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

        self.cp(local_files, os.path.join(bucket, ''), recursive=True)


class GCSNativeFileSystem(GCSFileSystem):
    """ File system interface that supports GCS and local files

    This uses the native python cloud storage library for read and write, rather than gsutil.
    The performance is significantly slower when doing any operations over several files (especially
    copy), but is thread-safe for applications which are already parallelized. It stores the files
    entirely in memory rather than using tempfiles.
    """
    def __init__(self, *args, **kwargs):
        self._client = None
        super(GCSNativeFileSystem, self).__init__(*args, **kwargs)

    def client(self):
        # Load client only when needed, so that this can be used for local paths without connecting
        if self._client is None:
            self._client = storage.Client()
        return self._client

    def ls(self, path):
        """ List all files at the specified path, supports globbing
        """
        logging.info('Globbing file content in {}'.format(path))

        # use GCSFileSystem's implementation for local paths
        if self.local(path):
            return super(GCSNativeFileSystem, self).ls(path)

        # find all names that start with the prefix for this path
        bucket, path = self._split(path)
        prefix = self._prefix(path)

        if prefix == path:
            # no pattern matching
            iterator = self._list_blobs(bucket, prefix=prefix, delimiter='/')
            names = [b.name for b in iterator]
            names += iterator.prefixes
            # if we ran ls('gs://bucket/dir') we need to rerun with '/' to get the content
            if names == [os.path.join(path, '')]:
                return self.ls(os.path.join('gs://' + bucket, path, ''))
        else:
            # we have a pattern to match
            names = [b.name for b in self._list_blobs(bucket, prefix=prefix)]
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
        return sorted(p.rstrip('/') for p in paths)

    def is_dir(self, path):
        # Check if a path is a directory, locally or on GCS
        if self.local(path):
            return os.path.isdir(path)
        elif path.endswith('/'):
            return True
        else:
            return self.ls(path) != [path]

    def copy_single(self, source, dest):
        local_source = self.local(source)
        local_dest = self.local(dest)

        if local_source and local_dest:
            shutil.copy(source, dest)

        if not local_source and local_dest:
            if os.path.isdir(dest):
                dest = os.path.join(dest, os.path.basename(source))
            self._blob(source).download_to_filename(dest)

        if local_source and not local_dest:
            if dest.endswith('/'):
                dest = os.path.join(dest, os.path.basename(source))
            self._blob(dest).upload_from_filename(source)

        if not local_source and not local_dest:
            if dest.endswith('/'):
                dest = os.path.join(dest, os.path.basename(source))
            self._transfer(source, dest)

    def cp(self, sources, dest, recursive=False):
        """ Copy the files in sources (recursively) to dest

        Parameters
        ----------
        sources : list of str
            The list of paths to copy, which can be directories
        dest : str
            The destination for the copy of source(s)
        recursive : bool, default False
            If true, recursively copy directories
        """
        if isinstance(sources, string_types):
            sources = [sources]

        for source in sources:
            if recursive and self.is_dir(source):
                # Note: if source ends with a '/', this copies the content into dest
                #   and if source does not, this copies the whole directory into dest
                #   this is the same behavior as copy
                subsource = [s.rstrip('/') for s in self.ls(source)]
                subdest = os.path.join(dest, os.path.basename(source), '')
                if self.local(subdest) and not os.path.exists(subdest):
                    os.makedirs(subdest)
                self.cp(subsource, subdest, recursive=True)
            else:
                self.copy_single(source, dest)

    def rm_single(self, path):
        if self.local(path):
            os.remove(path)
        else:
            self._blob(path).delete()

    def rm(self, paths, recursive=False):
        """ Remove the files at paths

        Parameters
        ----------
        paths : list of str
            The paths to remove
        recursive : bool, default False
            If true, recursively remove any directories
        """
        if isinstance(paths, string_types):
            paths = [paths]

        for path in paths:
            if recursive and self.is_dir(path) and not self.local(path):
                self.rm(self.ls(path), recursive=True)
            elif recursive and self.is_dir(path):
                shutil.rmtree(path)
            else:
                self.rm_single(path)

    @contextmanager
    def open(self, path, mode='rb'):
        """ Access paths as a file-like object

        Parameters
        ----------
        path: str
            The path of the file to access
        mode: str
            The file mode for the opened file

        Returns
        -------
        file: BytesIO
            A BytesIO handle for the specified path, works like a file object
        """
        datafile = DataFile(path, BytesIO())
        if mode.startswith('r'):
            self._read(datafile)
        if not mode.endswith('b') and PY3:
            handle = TextIOWrapper(datafile.handle)
        else:
            handle = datafile.handle

        yield handle

        if mode.startswith('w'):
            handle.seek(0)
            self._write(datafile)
        datafile.handle.close()

    def access(self, paths):
        """ Access multiple paths as file-like objects

        This allows for optimization like parallel downloads. To help track which files
        came from which objects, this returns instances of Datafile

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
            self._write(d)

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

    def _split(self, path):
        bucket = path.replace(self.GCS, '').split('/')[0]
        prefix = "gs://{}".format(bucket)
        path = path[len(prefix) + 1:]
        return bucket, path

    @_retry_with_backoff
    def _list_blobs(self, bucket, prefix=None, delimiter=None):
        return self.client().get_bucket(bucket).list_blobs(prefix=prefix, delimiter=delimiter)

    @_retry_with_backoff
    def _blob(self, path):
        bucket, path = self._split(path)
        return storage.Blob(path, self.client().get_bucket(bucket))

    @_retry_with_backoff
    def _transfer(self, path1, path2):
        bucket1, path1 = self._split(path1)
        bucket2, path2 = self._split(path2)
        source_bucket = self.client().get_bucket(bucket1)
        source_blob = source_bucket.blob(path1)
        destination_bucket = self.client().get_bucket(bucket2)
        source_bucket.copy_blob(source_blob, destination_bucket, path2)

    @_retry_with_backoff
    def _read(self, datafile):
        if self.local(datafile.path):
            with open(datafile.path, 'rb') as f:
                datafile.handle.write(f.read())
        else:
            self._blob(datafile.path).download_to_file(datafile.handle)
        datafile.handle.seek(0)

    @_retry_with_backoff
    def _write(self, datafile):
        datafile.handle.seek(0)
        if self.local(datafile.path):
            dirname = os.path.dirname(datafile.path)
            if not os.path.isdir(dirname):
                os.makedirs(dirname)

            with open(datafile.path, 'wb') as f:
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
