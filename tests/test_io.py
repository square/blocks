import json
import os
import pytest
import numpy as np
import blocks.io as io
from blocks.filesystem import DataFile


def test_read_write_native_formats(randomdata, datadir_local):
    extensions = ['.pkl', '.csv']
    for extension in extensions:
        path = os.path.join(datadir_local, 'tmp{}'.format(extension))
        df = _reload(randomdata, path)
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
    # add a unicode column name
    randomdata[u'f10'] = randomdata['f9']
    path = os.path.join(datadir_local, 'tmp{}'.format('.pq'))
    with pytest.warns(UserWarning):
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
    with open(path, 'wb') as f:
        json.dump(schema, f)


def _reload(data, path, **kwargs):
    with open(path, 'w') as f:
        d = DataFile(path, f)
        io.write_df(data, d, **kwargs)
    with open(path, 'r') as f:
        d = DataFile(path, f)
        df = io.read_df(d)
    return df
