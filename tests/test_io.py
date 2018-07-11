import os
import pytest
import numpy as np
import blocks.io as io


def test_read_write_native_formats(randomdata, datadir_local):
    extensions = ['.pkl', '.csv']
    for extension in extensions:
        path = os.path.join(datadir_local, 'tmp{}'.format(extension))
        io.write_df(randomdata, path)
        df = io.read_df(path)
        assert(np.isclose(df, randomdata).all().all())


def test_read_write_hdf(randomdata, datadir_local):
    pytest.importorskip('tables')
    path = os.path.join(datadir_local, 'tmp.h5')
    io.write_df(randomdata, path)
    df = io.read_df(path)
    assert(np.isclose(df, randomdata).all().all())


def test_read_write_parquet(randomdata, datadir_local):
    pytest.importorskip('pyarrow')
    pytest.importorskip('pandas', '0.22.0')
    path = os.path.join(datadir_local, 'tmp.pq')
    io.write_df(randomdata, path)
    df = io.read_df(path)
    assert(np.isclose(df, randomdata).all().all())


def test_read_write_parquet_unicode(randomdata, datadir_local):
    pytest.importorskip('pyarrow')
    pytest.importorskip('pandas', '0.22.0')
    # add a unicode column name
    randomdata[u'f10'] = randomdata['f9']
    path = os.path.join(datadir_local, 'tmp{}'.format('.pq'))
    with pytest.warns(UserWarning):
        io.write_df(randomdata, path)
    df = io.read_df(path)
    assert(np.isclose(df, randomdata).all().all())
