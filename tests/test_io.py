import json
import os
import pandas as pd
import numpy as np
import pytest
import six
import blocks.dfio as io
from blocks.filesystem import DataFile


def test_read_write_native_formats(randomdata, datadir_local):
    extensions = ['.pkl', '.csv', '.json', '.json.gz', '.pkl.gz', '.csv.gz']
    for extension in extensions:
        path = os.path.join(datadir_local, 'tmp{}'.format(extension))
        df = _reload(randomdata, path)
        assert(np.isclose(df, randomdata).all().all())


def test_compression_csv(randomdata, datadir_local):
    path = os.path.join(datadir_local, 'tmp.csv.gz')
    # write compressed with pandas
    randomdata.to_csv(path, index=False, compression='gzip')

    # read compressed with blocks
    with open(path, 'rb') as f:
        d = DataFile(path, f)
        df = io.read_df(d)
    assert(np.isclose(df, randomdata).all().all())

    # write compressed with blocks
    with open(path, 'wb') as f:
        d = DataFile(path, f)
        io.write_df(randomdata, d)
    # read compressed with pandas
    df = pd.read_csv(path, compression='gzip')
    assert(np.isclose(df, randomdata).all().all())


def test_compression_pickle(randomdata, datadir_local):
    path = os.path.join(datadir_local, 'tmp.pkl.gz')
    # write compressed with pandas
    randomdata.to_pickle(path, compression='gzip')

    # read compressed with blocks
    with open(path, 'rb') as f:
        d = DataFile(path, f)
        df = io.read_df(d)
    assert(np.isclose(df, randomdata).all().all())

    # write compressed with blocks
    with open(path, 'wb') as f:
        d = DataFile(path, f)
        io.write_df(randomdata, d)
    # read compressed with pandas
    df = pd.read_pickle(path, compression='gzip')
    assert(np.isclose(df, randomdata).all().all())


def test_compression_parquet(randomdata, datadir_local):
    pytest.importorskip('pyarrow')
    pytest.importorskip('pandas', minversion='0.22.0')
    path = os.path.join(datadir_local, 'tmp.parquet.gz')
    # write compressed with pandas
    randomdata.to_parquet(path, compression='gzip')

    # read compressed with blocks
    with open(path, 'rb') as f:
        d = DataFile(path, f)
        df = io.read_df(d)
    assert(np.isclose(df, randomdata).all().all())

    # write compressed with blocks
    with open(path, 'wb') as f:
        d = DataFile(path, f)
        io.write_df(randomdata, d)
    # read compressed with pandas
    df = pd.read_parquet(path)
    assert(np.isclose(df, randomdata).all().all())


def test_read_write_parquet(randomdata, datadir_local):
    pytest.importorskip('pyarrow')
    pytest.importorskip('pandas', minversion='0.22.0')
    path = os.path.join(datadir_local, 'tmp.pq')
    df = _reload(randomdata, path)
    assert(np.isclose(df, randomdata).all().all())


def test_read_write_parquet_unicode(randomdata, datadir_local):
    pytest.importorskip('pyarrow')
    pytest.importorskip('pandas', minversion='0.22.0')
    randomdata[u'f10'] = randomdata['f9']
    path = os.path.join(datadir_local, 'tmp{}'.format('.pq'))
    if six.PY2:
        # expect a warning about this in python2
        with pytest.warns(UserWarning):
            df = _reload(randomdata, path)
    else:
        df = _reload(randomdata, path)
    assert(np.isclose(df, randomdata).all().all())


def test_read_write_avro(randomdata, datadir_local):
    pytest.importorskip('fastavro')
    path = os.path.join(datadir_local, 'tmp.avro')
    schema_path = os.path.join(datadir_local, 'tmp.avsc')
    _write_schema(randomdata, schema_path)
    df = _reload(randomdata, path, schema_path=schema_path)
    assert(np.isclose(df, randomdata).all().all())


def _write_schema(data, path):
    schema = {"type": "record", "name": "testschema"}
    schema["fields"] = [{"name": str(c), "type": "float"} for c in data.columns.values]
    with open(path, 'w') as f:
        json.dump(schema, f)


def _reload(data, path, **kwargs):
    with open(path, 'wb') as f:
        d = DataFile(path, f)
        io.write_df(data, d, **kwargs)
    with open(path, 'rb') as f:
        d = DataFile(path, f)
        df = io.read_df(d)
    return df
