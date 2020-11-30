import warnings

from blocks.filesystem.base import FileSystem


class GCSFileSystem(FileSystem):
    """Deprecated, see FileSystem"""

    def __init__(self, *args, **kwargs):
        warnings.warn(
            "This class is deprecated, use blocks.filesystem.FileSystem",
            DeprecationWarning,
        )
        super().__init__(*args, **kwargs)
