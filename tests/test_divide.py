import numpy as np
import blocks


def test_divide(datadir, randomdata, fs):
    blocks.divide(randomdata, datadir, 10, extension='.pkl')
    assert(len(fs.ls(datadir)) == 10)
    df = blocks.assemble(datadir)
    assert(np.isclose(df, randomdata).all().all())


def test_divide_offset(datadir, randomdata, fs):
    blocks.divide(randomdata, datadir, 10, extension='.pkl')
    blocks.divide(randomdata, datadir, 10, 10, extension='.pkl')
    assert(len(fs.ls(datadir)) == 20)
    df = blocks.assemble(datadir)
    expected = randomdata.append(randomdata)
    assert(np.isclose(df, expected).all().all())


def test_divide_cgroups(datadir, randomdata, fs):
    randomdata.insert(0, 'key', list(range(10)))
    cgroups_columns = {
        'cgroup1': ['key', 'f0', 'f1', 'f2'],
        'cgroup2': ['key', 'f3', 'f4', 'f5'],
        'cgroup3': ['key', 'f6', 'f7', 'f8', 'f9'],
    }

    blocks.divide(
        randomdata,
        datadir,
        10,
        cgroup_columns=cgroups_columns,
        extension='.pkl'
    )
    assert(len(fs.ls(datadir)) == 3)
    for subdir in fs.ls(datadir):
        assert(len(fs.ls(subdir)) == 10)
    df = blocks.assemble(datadir)
    assert(df.equals(randomdata))
