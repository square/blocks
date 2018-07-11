import os
import numpy as np
import blocks


def test_place(datadir, randomdata, fs):
    blocks.place(randomdata, os.path.join(datadir, 'example.csv'))
    assert(len(fs.ls(datadir)) == 1)
    df = blocks.assemble(datadir)
    assert(np.isclose(df, randomdata).all().all())
