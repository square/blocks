import gzip
import os
import six
import warnings
import pandas as pd
from io import TextIOWrapper

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
    filetype, compression = _get_extension(datafile.path)
    reader = _readers[filetype]
    if reader in (pd.read_csv, pd.read_pickle, pd.read_json) and compression is not None:
        read_args["compression"] = compression
    elif reader in (pd.read_csv, pd.read_pickle, pd.read_json):
        read_args["compression"] = None  # default "infer" incompatible with handles as of 0.24
    if reader == pd.read_json:
        # Default json file is newline delimited json records, but can be overwritten
        defaults = {'lines': True, 'orient': 'records'}
        defaults.update(read_args)
        read_args = defaults
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
    # infer compression from filepath or from explicit arg
    compression = compression or write_args.get('compression')
    buffer = datafile.handle

    # Some customizations for different file types
    if write_name == 'to_avro':
        return _write_avro(df, buffer, **write_args)

    if write_name == 'to_parquet' and not pd.Series(df.columns).map(type).eq(str).all():
        warnings.warn(
            'Dataframe contains non-string column names, which cannot be saved in parquet.\n'
            'Blocks will attempt to convert them to strings.'
        )
        df.columns = df.columns.astype('str')

    if write_name == 'to_json':
        defaults = {'lines': True, 'orient': 'records'}
        defaults.update(write_args)
        write_args = defaults

    if write_name == 'to_csv':
        # make index=False the default for similar behaviour to other formats
        write_args['index'] = write_args.get('index', False)

    # For csv and pickle we have to manually compress
    manual_compress = write_name in ('to_csv', 'to_pickle', 'to_json')
    if manual_compress:
        write_args['compression'] = None  # default "infer" incompatible with handles as of 0.24

    if manual_compress and compression == 'gzip':
        buffer = gzip.GzipFile(fileobj=buffer, mode='w')
    elif manual_compress and compression is not None:
        raise ValueError('Compression {} is not supported for CSV/Pickle'.format(compression))

    # And for 23 compatibility need a textiowrapper for text formats
    if write_name in ('to_csv', 'to_json') and six.PY3:
        buffer = TextIOWrapper(buffer)

    write_fn = getattr(df, write_name)
    return write_fn(buffer, **write_args)


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
    return ext, _compressions.get(comp)


_readers = {
    '.pq': pd.read_parquet,
    '.parquet': pd.read_parquet,
    '.csv': pd.read_csv,
    '.pkl': pd.read_pickle,
    '.avro': _read_avro,
    '.json': pd.read_json,
}


_writers = {
    '.pq': 'to_parquet',
    '.parquet': 'to_parquet',
    '.csv': 'to_csv',
    '.pkl': 'to_pickle',
    '.avro': 'to_avro',
    '.json': 'to_json',
}

_compressions = {
    '.gz': 'gzip',
    '.bz2': 'bz2',
    '.zip': 'zip',
    '.xz': 'xz'
}
