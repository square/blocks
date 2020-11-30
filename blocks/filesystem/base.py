from typing import Union, Sequence, Tuple, List
from collections import defaultdict
from fsspec.core import split_protocol, get_filesystem_class, has_magic


class FileSystem:
    """Filesystem for manipulating files in the cloud

    This supports operations on local files and any other protocol supported by fsspec.
    This is a wrapper to fsspec which provides backwards compatibility for blocks filesystems
    and a simplified interface.

    Parameters
    ----------
    storage_options: Mapping[str, Mapping[str, Any]]
        Additional options passed to each filesystem for each protocol
        e.g. {'gs': {'project': 'example'}} to set the gs filesytem project to example
    """

    def __init__(self, **storage_options):
        self.storage_options = defaultdict(dict)
        self.storage_options.update(storage_options)
        self.storage_options[None]["auto_mkdir"] = True
        self.filesystems = {}

    def _get_protocol_path(self, urlpath) -> Tuple[str, List[str]]:
        if isinstance(urlpath, str):
            return split_protocol(urlpath)

        protocols, paths = zip(*map(split_protocol, urlpath))
        assert (
            len(set(protocols)) == 1
        ), "Cannot mix file protocols in a single operation"
        return protocols[0], list(paths)

    def _get_filesystem(self, protocol):
        if protocol not in self.filesystems:
            self.filesystems[protocol] = get_filesystem_class(protocol)(
                **self.storage_options[protocol]
            )
        return self.filesystems[protocol]

    def ls(self, path: str) -> Sequence[str]:
        """List files correspond to path, including glob wildcards

        Parameters
        ----------
        path : str
            The path to the file or directory to list; supports wildcards
        """
        protocol, path = self._get_protocol_path(path)
        fs = self._get_filesystem(protocol)
        try:
            if has_magic(path):
                output = fs.glob(path)
            else:
                output = fs.ls(path)
        # TODO fix in base
        except FileNotFoundError:
            return []
        except NotADirectoryError:
            return [path]

        if protocol is not None:
            output = ["://".join([protocol, path]) for path in output]
        return sorted(output)

    def copy(
        self,
        sources: Union[str, Sequence[str]],
        dest: Union[str, Sequence[str]],
        recursive=False,
    ):
        """Copy the files in sources to dest

        Parameters
        ----------
        sources : list of str
            The list of paths to copy
        dest : str
            The destination(s) for the copy of source(s)
        recursive : bool
            If true, recursively copy any directories
        """
        if isinstance(sources, str):
            sources = [sources]

        ps, sources = self._get_protocol_path(sources)
        pd, dest = self._get_protocol_path(dest)

        if ps == pd:
            fs = self._get_filesystem(ps)

            # Temporary workaround for a bug in gcsfs
            if ps == "gs" and recursive:
                sources = fs.expand_path(sources, recursive=True)
                sources = [s for s in sources if not fs.isdir(s)]
                return fs.copy(sources, dest, recursive=False)

            fs.copy(sources, dest, recursive=recursive)

        if ps is None:
            fs = self._get_filesystem(pd)
            fs.put(sources, dest, recursive=recursive)

        if pd is None:
            fs = self._get_filesystem(ps)
            fs.get(sources, dest, recursive=recursive)

        if pd == "gs":
            if isinstance(dest, str):
                fs.invalidate_cache(dest)
            else:
                for d in dest:
                    fs.invalidate_cache(d)

        if pd is not None and ps is not None:
            raise NotImplementedError(
                "Cannot do direct copy between two cloud filesystems"
            )

    def remove(self, paths: Union[str, List[str]], recursive: bool = False):
        """Remove the files at paths

        Parameters
        ----------
        paths : list of str
            The paths to remove
        recursive : bool, default False
            If true, recursively remove any directories
        """
        protocol, paths = self._get_protocol_path(paths)
        fs = self._get_filesystem(protocol)

        if protocol is None and not isinstance(paths, str):
            # TODO should local not just handle this?
            for path in paths:
                fs.rm(path, recursive=recursive)
        else:
            return fs.rm(paths, recursive=recursive)

    def open(self, path: str, mode="rb", **kwargs):
        """Return a file-like object from the filesystem

        The resultant instance must function correctly in a context ``with``
        block.

        Parameters
        ----------
        path: str
            Target file
        mode: str like 'rb', 'w'
            See builtin ``open()``
        kwargs:
            Forwarded to the filesystem implementation
        """
        protocol, path = self._get_protocol_path(path)
        fs = self._get_filesystem(protocol)
        return fs.open(path, mode, **kwargs)

    def isdir(self, path: str):
        """Check if the path is a directory"""
        protocol, path = self._get_protocol_path(path)
        fs = self._get_filesystem(protocol)
        return fs.isdir(path)

    def mkdir(self, path: str):
        """Make directory at path"""
        protocol, path = self._get_protocol_path(path)
        fs = self._get_filesystem(protocol)
        return fs.mkdir(path)

    # Aliases
    cp = copy
    rm = remove
