import os
import warnings
import pandas as pd


try:
    import pyarrow.parquet as pq
    parquet = True
except ImportError:
    parquet = False


def read_df(path, **read_args):
    """ Read a dataframe from file based on the file extension

    The following formats are supported:
    parquet, csv, hdf5, pickle

    Parameters
    ----------
    path : str
        Path to the file
    read_args : optional
        All keyword args are passed to the read function

    Returns
    -------
    data : pd.DataFrame

    Notes
    -----
    The read functions are taken from pandas, e.g. pd.read_csv
    Check the pandas doc for more information on the supported arguments

    """
    return _readers[_get_extension(path)](path, **read_args)


def write_df(df, path, **write_args):
    """ Write a dataframe to file based on the file extension

    The following formats are supported:
    parquet, csv, hdf5, pickle

    Parameters
    ----------
    df : pd.DataFrame
        The dataframe to write to disk
    path : str
        Path to the file
    write_args : optional
        All keyword args are passed to the write function

    Notes
    -----
    The write functions are taken from pandas, e.g. pd.to_csv
    Check the pandas doc for more information on the supported arguments

    """
    write_name = _writers[_get_extension(path)]

    if write_name == 'to_parquet' and not pd.Series(df.columns).map(type).eq(str).all():
        warnings.warn(
            'Dataframe contains non-string column names, which cannot be saved in parquet.\n'
            'Blocks will attempt to convert them to strings.'
        )
        df.columns = df.columns.astype('str')

    write_fn = getattr(df, write_name)
    if write_name == 'to_hdf':
        # hdf requires a 'key' argument
        return write_fn(path, 'data', **write_args)
    elif write_name == 'to_csv':
        # make index=False the default for similar behaviour to other formats
        csvargs = {'index': False}
        csvargs.update(write_args)
        return write_fn(path, **csvargs)
    else:
        return write_fn(path, **write_args)


def _read_parquet(path, **read_args):
    """ Read a dataframe from parquet file

    Parameters
    ----------
    path : str
        Path to the parquet file
    read_args : optional
        All keyword args are passed to pyarrow.parquet.read_table

    Returns
    -------
    data : pd.DataFrame

    """
    if not parquet:
        raise ImportError('Parquet support requires pyarrow.\n'
                          'Install blocks with the [pq] option or `pip install pyarrow`')
    return pq.read_table(path, **read_args).to_pandas()


def _get_extension(path):
    name, ext = os.path.splitext(path)
    # Support compression extensions, eg part.csv.gz
    if ext in _compressions and '.' in name:
        return _get_extension(name)
    return ext


_readers = {
    '.pq': _read_parquet,
    '.parquet': _read_parquet,
    '.h5': pd.read_hdf,
    '.hdf': pd.read_hdf,
    '.hdf5': pd.read_hdf,
    '.csv': pd.read_csv,
    '.pkl': pd.read_pickle,
}


_writers = {
    '.pq': 'to_parquet',
    '.parquet': 'to_parquet',
    '.h5': 'to_hdf',
    '.hdf': 'to_hdf',
    '.hdf5': 'to_hdf',
    '.csv': 'to_csv',
    '.pkl': 'to_pickle',
}

_compressions = [
    '.gz',
    '.bz2',
    '.zip',
    '.xz'
]
