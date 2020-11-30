## [0.9.0b0] - 2020-11-30

### Removed

- GCS(Native)FileSystem no longer provides store/access
- GCSFileSystem are now backwards compatibility wrappers for FileSystem and will
  be removed in 1.0.0
- No more explicit compression support, compression may still be possible
  through read/write args

### Added
 
- New generic FileSystem backed by fsspec 
  - rather than using fsspec directly we use this wrapper for better backwards
    compatibility and more automatic protocol handling
  - In theory any fsspec implementation is supported but only local and gcsfs
    are tested so far

### Changed
- We now use paths (rather than file objects) in pandas io methods for better
  compatibility
- All GCS operations are handled through gcsfs, which has much better
  performance with large numbers of files and has been more robust to connection
  errors
- Globbing must now expand to match patterns to literal files, not directories

## [0.8.0] - 2020-10-14

### Removed

- Dropped Python 2 support.
- Compression on write no longer supported by Pandas

### Added

- Typehints for Python 3
- some missing abstract methods to the base FileSystem class definition.

## [0.7.1] - 2020-08-20

### Added

`blocks.pickle` and `blocks.unpickle` utilities to save and load pickle files.

## [0.7.0] - 2020-07-22

This release has minor backwards incompatible for anyone that directly used
datafiles. The top level and filesystem APIs (assemble, iterate, partitioned,
etc) are unchanged.

### Added

- LocalDataFile that implements datafile for local paths
- GCSNativeDataFile that implements datafile for GCS paths using GCS python blob API

### Changed

- The old datafile namedtuple is now an abstract base class
- Datafiles now use a contextmanager for handle, which yields a file handle
- Datafiles are only opened one at a time just before the data is loaded into
  memory
  - This should prevent exceeding the os open file limit with large directories
  - Also sets the stage for better multithreading support
