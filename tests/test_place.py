import os
import numpy as np
import blocks


def test_place(datadir, randomdata, fs):
    blocks.place(randomdata, os.path.join(datadir, "example.parquet"), filesystem=fs)
    assert len(fs.ls(datadir)) == 1
    df = blocks.assemble(os.path.join(datadir, "*"), filesystem=fs)
    assert np.isclose(df, randomdata).all().all()
