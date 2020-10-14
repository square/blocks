import fnmatch
import logging
import os
import shutil
import time
from contextlib import contextmanager
from io import BytesIO, TextIOWrapper

import requests
import wrapt
from blocks.datafile import DataFile, LocalDataFile
from blocks.filesystem.gcs_filesystem import GCSFileSystem
from google.cloud import storage


class GCSNativeDataFile(DataFile):
    def __init__(self, path, filesystem):
        self.filesystem = filesystem
        super(GCSNativeDataFile, self).__init__(path)

    @contextmanager
    def handle(self, mode="rb"):
        bytesio = BytesIO()

        if mode.startswith("r"):
            self.filesystem._read(self.path, bytesio)

        if mode.endswith("b"):
            buf = bytesio
        else:
            buf = TextIOWrapper(bytesio)

        yield buf

        if mode.startswith("w"):
            self.filesystem._write(self.path, bytesio)


@wrapt.decorator
def _retry_with_backoff(wrapped, instance, args, kwargs):
    trial = 0
    while True:
        wait = 2 ** (trial + 2)  # 4s up to 128s
        try:
            return wrapped(*args, **kwargs)
        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.ChunkedEncodingError,
        ):
            if trial == 6:
                raise
            logging.info(
                "{} failed to connect, retrying after {}s".format(
                    wrapped.__name__, wait
                )
            )
            trial += 1
        time.sleep(wait)


class GCSNativeFileSystem(GCSFileSystem):
    """File system interface that supports GCS and local files

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
        """List all files at the specified path, supports globbing"""
        logging.info("Globbing file content in {}".format(path))

        # use GCSFileSystem's implementation for local paths
        if self.local(path):
            return super(GCSNativeFileSystem, self).ls(path)

        # find all names that start with the prefix for this path
        bucket, path = self._split(path)
        prefix = self._prefix(path)

        if prefix == path:
            # no pattern matching
            iterator = self._list_blobs(bucket, prefix=prefix, delimiter="/")
            names = [b.name for b in iterator]
            names += iterator.prefixes
            # if we ran ls('gs://bucket/dir') we need to rerun with '/' to get the content
            if names == [os.path.join(path, "")]:
                return self.ls(os.path.join("gs://" + bucket, path, ""))
        else:
            # we have a pattern to match
            names = [b.name for b in self._list_blobs(bucket, prefix=prefix)]
            # for recursive glob, do not attempt to match folders, only files
            if "**" in path:
                names = fnmatch.filter(names, path)
            else:
                # make sure we can match folders in addition to blobs
                candidates = set(names) | set(self._list_single(prefix, names))
                names = fnmatch.filter(candidates, path)
                # one more filter because fnmatch recurses single *
                names = [name for name in names if name.count("/") == path.count("/")]

        paths = ["gs://{}/{}".format(bucket, name) for name in names]
        return sorted(p.rstrip("/") for p in paths)

    def is_dir(self, path):
        # Check if a path is a directory, locally or on GCS
        if self.local(path):
            return os.path.isdir(path)
        elif path.endswith("/"):
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
            if dest.endswith("/"):
                dest = os.path.join(dest, os.path.basename(source))
            self._blob(dest).upload_from_filename(source)

        if not local_source and not local_dest:
            if dest.endswith("/"):
                dest = os.path.join(dest, os.path.basename(source))
            self._transfer(source, dest)

    def cp(self, sources, dest, recursive=False):
        """Copy the files in sources (recursively) to dest

        Parameters
        ----------
        sources : list of str
            The list of paths to copy, which can be directories
        dest : str
            The destination for the copy of source(s)
        recursive : bool, default False
            If true, recursively copy directories
        """
        if isinstance(sources, str):
            sources = [sources]

        for source in sources:
            if recursive and self.is_dir(source):
                # Note: if source ends with a '/', this copies the content into dest
                #   and if source does not, this copies the whole directory into dest
                #   this is the same behavior as copy
                subsource = [s.rstrip("/") for s in self.ls(source)]
                subdest = os.path.join(dest, os.path.basename(source), "")
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
        """Remove the files at paths

        Parameters
        ----------
        paths : list of str
            The paths to remove
        recursive : bool, default False
            If true, recursively remove any directories
        """
        if isinstance(paths, str):
            paths = [paths]

        for path in paths:
            if recursive and self.is_dir(path) and not self.local(path):
                self.rm(self.ls(path), recursive=True)
            elif recursive and self.is_dir(path):
                shutil.rmtree(path)
            else:
                self.rm_single(path)

    def datafile(self, path):
        if self.local(path):
            return LocalDataFile(path, path)
        return GCSNativeDataFile(path, self)

    @contextmanager
    def open(self, path, mode="rb"):
        """Access paths as a file-like object

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
        with self.datafile(path).handle(mode) as handle:
            yield handle

    def access(self, paths):
        """Access multiple paths as file-like objects

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
            datafiles.append(self.datafile(path))
        return datafiles

    @contextmanager
    def store(self, bucket, files):
        """Create file stores that will be written to the filesystem on close

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
            datafiles.append(self.datafile(os.path.join(bucket, f)))

        yield datafiles

    def _prefix(self, path):
        # find the lowest prefix that doesn't include a pattern match
        splits = path.split("/")
        accumulate = []
        while splits:
            sub = splits.pop(0)
            if any(x in sub for x in ["*", "?", "[", "]"]):
                break
            accumulate.append(sub)
        return "/".join(accumulate) if accumulate else None

    def _list_single(self, prefix, names):
        # find files that are directly in the dir specified by prefix, not in subfolders
        valid = set(n.replace(prefix, "").lstrip("/").split("/")[0] for n in names)
        return [os.path.join(prefix, n) for n in valid]

    def _split(self, path):
        bucket = path.replace(self.GCS, "").split("/")[0]
        prefix = "gs://{}".format(bucket)
        path = path[len(prefix) + 1 :]
        return bucket, path

    @_retry_with_backoff
    def _list_blobs(self, bucket, prefix=None, delimiter=None):
        return (
            self.client()
            .get_bucket(bucket)
            .list_blobs(prefix=prefix, delimiter=delimiter)
        )

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
    def _read(self, path, bytesio):
        self._blob(path).download_to_file(bytesio)
        bytesio.seek(0)

    @_retry_with_backoff
    def _write(self, path, bytesio):
        try:
            bytesio.seek(0)
        except ValueError:
            raise ValueError(
                "The pandas function that attempted to write this file cleared the memory before blocks"
                " could write it, this is a known issue with an upcoming fix. Try a non-pickle file type"
                " or the default filesystem."
            )
        self._blob(path).upload_from_file(bytesio)
