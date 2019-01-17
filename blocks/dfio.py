import os
import six
import warnings
import pandas as pd
from io import TextIOWrapper

try:
    import pyarrow.parquet as pq
    parquet = True
except ImportError:
    parquet = False

try:
    import fastavro as avro
    avro_imported = True
except ImportError:
    avro_imported = False


def read_df(datafile, **read_args):
    """ Read a dataframe from file based on the file extension

    The following formats are supported:
    parquet, avro, csv, pickle

    Parameters
    ----------
    datafile : DataFile
        DataFile instance with the path and handle
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
    datafile.handle.seek(0)  # ensure we start from the beginning of the file
    filetype, compression = _get_extension(datafile.path)
    reader = _readers[filetype]
    if reader in (pd.read_csv, pd.read_pickle) and compression is not None:
        read_args["compression"] = compression
    return reader(datafile.handle, **read_args)


def write_df(df, datafile, **write_args):
    """ Write a dataframe to file based on the file extension

    The following formats are supported:
    parquet, avro, csv, pickle

    Parameters
    ----------
    df : pd.DataFrame
        The dataframe to write to disk
    datafile : DataFile
        Datafile instance with the path and file handle
    write_args : optional
        All keyword args are passed to the write function

    Notes
    -----
    The write functions are taken from pandas, e.g. pd.to_csv
    Check the pandas doc for more information on the supported arguments

    """
    extension, compression = _get_extension(datafile.path)
    write_name = _writers[extension]

    if write_name == 'to_parquet' and not pd.Series(df.columns).map(type).eq(str).all():
        warnings.warn(
            'Dataframe contains non-string column names, which cannot be saved in parquet.\n'
            'Blocks will attempt to convert them to strings.'
        )
        df.columns = df.columns.astype('str')
    if write_name == 'to_avro':
        return _write_avro(df, datafile.handle, **write_args)
    write_fn = getattr(df, write_name)
    if write_name == 'to_csv':
        # make index=False the default for similar behaviour to other formats
        csvargs = {'index': False}
        csvargs.update(write_args)
        if compression is not None:
            csvargs["compression"] = compression
        buffer = datafile.handle if six.PY2 else TextIOWrapper(datafile.handle)
        return write_fn(buffer, **csvargs)
    else:
        if compression is not None:
            write_args["compression"] = compression
        return write_fn(datafile.handle, **write_args)


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


def _read_avro(handle, **read_args):
    if not avro_imported:
        raise ImportError('Avro support requires fastavro.\n'
                          'Install blocks with the [avro] option or `pip install fastavro`')
    records = []
    avro_reader = avro.reader(handle)
    for record in avro_reader:
        records.append(record)
    return pd.DataFrame.from_dict(records)


def _write_avro(df, handle, **write_args):
    if not avro_imported:
        raise ImportError('Avro support requires fastavro.\n'
                          'Install blocks with the [avro] option or `pip install fastavro`')
    schema = None
    schema_path = None
    try:
        schema = write_args['schema']
    except KeyError:
        try:
            schema_path = write_args['schema_path']
        except KeyError:
            raise Exception("You must provide a schema or schema path when writing to Avro")
    if schema is None:
        schema = avro.schema.load_schema(schema_path)
    records = df.to_dict('records')
    avro.writer(handle, schema, records)


def _get_extension(path):
    name, ext = os.path.splitext(path)
    # Support compression extensions, eg part.csv.gz
    comp = None
    if ext in _compressions and '.' in name:
        comp = ext
        name, ext = os.path.splitext(name)
    return ext, comp


_readers = {
    '.pq': _read_parquet,
    '.parquet': _read_parquet,
    '.csv': pd.read_csv,
    '.pkl': pd.read_pickle,
    '.avro': _read_avro,
}


_writers = {
    '.pq': 'to_parquet',
    '.parquet': 'to_parquet',
    '.csv': 'to_csv',
    '.pkl': 'to_pickle',
    '.avro': 'to_avro',
}

_compressions = [
    '.gz',
    '.bz2',
    '.zip',
    '.xz'
]
