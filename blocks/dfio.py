import os
import warnings
import pandas as pd

try:
    import fastavro as avro

    avro_imported = True
except ImportError:
    avro_imported = False


def read_df(path, **read_args):
    """Read a dataframe path based on the file extension
    parquet, avro, csv, pickle, json

    Parameters
    ----------
    path: str
        The path to the file holding data
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
    filetype = _get_extension(path)
    reader = _readers[filetype]
    if reader == pd.read_json:
        # Default json file is newline delimited json records, but can be overwritten
        defaults = {"lines": True, "orient": "records"}
        defaults.update(read_args)
        read_args = defaults

    return reader(path, **read_args)


def write_df(df, path, **write_args):
    """Write a dataframe to file based on the file extension

    The following formats are supported:
    parquet, avro, csv, pickle, json

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
    extension = _get_extension(path)
    write_name = _writers[extension]

    # Some customizations for different file types
    if write_name == "to_avro":
        return _write_avro(df, path, **write_args)

    if write_name == "to_parquet" and not pd.Series(df.columns).map(type).eq(str).all():
        warnings.warn(
            "Dataframe contains non-string column names, which cannot be saved in parquet.\n"
            "Blocks will attempt to convert them to strings."
        )
        df.columns = df.columns.astype("str")

    if write_name == "to_json":
        defaults = {"lines": True, "orient": "records"}
        defaults.update(write_args)
        write_args = defaults

    if write_name == "to_csv":
        # make index=False the default for similar behaviour to other formats
        write_args["index"] = write_args.get("index", False)

    write_fn = getattr(df, write_name)
    write_fn(path, **write_args)


def _read_avro(path, **read_args):
    if not avro_imported:
        raise ImportError(
            "Avro support requires fastavro.\n"
            "Install blocks with the [avro] option or `pip install fastavro`"
        )
    records = []
    with open(path, "rb") as f:
        avro_reader = avro.reader(f)
        for record in avro_reader:
            records.append(record)
    return pd.DataFrame.from_dict(records)


def _write_avro(df, path, **write_args):
    if not avro_imported:
        raise ImportError(
            "Avro support requires fastavro.\n"
            "Install blocks with the [avro] option or `pip install fastavro`"
        )
    schema = None
    schema_path = None
    try:
        schema = write_args["schema"]
    except KeyError:
        try:
            schema_path = write_args["schema_path"]
        except KeyError:
            raise Exception(
                "You must provide a schema or schema path when writing to Avro"
            )
    if schema is None:
        schema = avro.schema.load_schema(schema_path)
    records = df.to_dict("records")
    with open(path, "wb") as f:
        avro.writer(f, schema, records)


def _get_extension(path):
    name, ext = os.path.splitext(path)
    return ext


_readers = {
    ".pq": pd.read_parquet,
    ".parquet": pd.read_parquet,
    ".csv": pd.read_csv,
    ".pkl": pd.read_pickle,
    ".avro": _read_avro,
    ".json": pd.read_json,
}


_writers = {
    ".pq": "to_parquet",
    ".parquet": "to_parquet",
    ".csv": "to_csv",
    ".pkl": "to_pickle",
    ".avro": "to_avro",
    ".json": "to_json",
}
