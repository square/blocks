import os
import blocks


def test_pickle(fs, temp):
    original = {"example": 0.0}
    path = os.path.join(temp, "test.pkl")
    blocks.pickle(original, path, filesystem=fs)
    unpickled = blocks.unpickle(path, filesystem=fs)
    assert original == unpickled
