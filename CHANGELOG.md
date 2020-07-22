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
