import atexit
import glob
import logging
import os
import shutil
import subprocess
import tempfile
from contextlib import contextmanager

from blocks.datafile import LocalDataFile
from blocks.filesystem.base import FileSystem
from six import string_types


class GCSFileSystem(FileSystem):
    """ File system interface that supports both local and GCS files

    This implementation uses subprocess and gsutil, which has excellent performance.
    However this can lead to problems in very multi-threaded applications and might not be
    as portable. For a python native implementation use GCSNativeFileSystem
    """

    GCS = "gs://"

    def __init__(self, parallel=True, quiet=True):
        flags = []
        if parallel:
            flags.append("-m")
        if quiet:
            flags.append("-q")
        self.gcscp = ["gsutil"] + flags + ["cp"]

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
        logging.info("Globbing file content in {}".format(path))
        if not self.local(path):
            with open(os.devnull, "w") as DEVNULL:
                p = subprocess.Popen(
                    ["gsutil", "ls", path],
                    stdout=subprocess.PIPE,
                    stderr=DEVNULL,
                    universal_newlines=True,
                )
                stdout = p.communicate()[0]
            output = [line for line in stdout.split("\n") if line and line[-1] != ":"]
        elif "**" in path:
            # Manual recursive glob, since in 2.X glob doesn't have recursive support
            path = path.rstrip("*")
            output = []
            for root, subdirs, files in os.walk(path):
                for fname in files:
                    output.append(os.path.join(root, fname))
        elif os.path.isdir(path):
            output = [os.path.join(path, f) for f in os.listdir(path)]
        else:
            output = glob.glob(path)
        return sorted(p.rstrip("/") for p in output)

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
            cmd = ["gsutil", "-m", "rm"]

        else:
            cmd = ["rm"]

        if recursive:
            cmd.append("-r")

        CHUNK_SIZE = 1000
        paths_chunks = [
            paths[x : x + CHUNK_SIZE] for x in range(0, len(paths), CHUNK_SIZE)
        ]
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

        summary = ", ".join(sources)
        logging.info("Copying {} to {}...".format(summary, dest))

        if any(self.GCS in x for x in sources + [dest]):
            # at least one location is on GCS
            cmd = self.gcscp
        else:
            cmd = ["cp"]

        if recursive:
            cmd.append("-r")

        CHUNK_SIZE = 1000
        sources_chunks = [
            sources[x : x + CHUNK_SIZE] for x in range(0, len(sources), CHUNK_SIZE)
        ]
        for sources in sources_chunks:
            subprocess.check_call(cmd + sources + [dest])

    @contextmanager
    def open(self, path, mode="rb"):
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
            if mode.startswith("r"):
                self.cp(path, nf.name)

            nf.seek(0)

            with open(nf.name, mode) as f:
                yield f

            nf.seek(0)

            if mode.startswith("w"):
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
            datafiles.append(LocalDataFile(path, local))
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
            datafiles.append(LocalDataFile(os.path.join(bucket, f), local))

        yield datafiles

        if self.local(bucket) and not os.path.exists(bucket):
            os.makedirs(bucket)

        self.cp(local_files, os.path.join(bucket, ""), recursive=True)


def _session_tempdir():
    """ Create a tempdir that will be cleaned up at session exit
    """
    tmpdir = tempfile.mkdtemp()
    # create and use a subdir of specified name to preserve cgroup logic
    atexit.register(lambda: shutil.rmtree(tmpdir))
    return tmpdir
