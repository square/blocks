import os
import pytest
import pandas as pd
import numpy as np

from blocks.filesystem import GCSFileSystem, GCSNativeFileSystem
from delegator import run

BUCKET = 'gs://blocks-example'

if os.environ.get('CI'):
    inputs = ['local']
    outputs = ['local']
    temps = ['local']
    filesystems = ['gcs']
else:
    inputs = ['local', 'gcs', 'gcs_extra']
    outputs = ['local', 'gcs']
    temps = ['local', 'gcs']
    filesystems = ['gcs', 'native']


@pytest.fixture(scope='session', params=filesystems)
def fs(request):
    if request.param == 'gcs':
        return GCSFileSystem()

    if request.param == 'native':
        return GCSNativeFileSystem()


@pytest.fixture(scope='function', params=temps)
def temp(request, tmpdir_factory):
    if request.param == 'local':
        path = str(tmpdir_factory.mktemp('temp'))
        yield path

    if request.param == 'gcs':
        path = os.path.join(BUCKET, 'temp')
        yield path
        run('gsutil -m rm -r {}'.format(path))


@pytest.fixture(scope='session')
def populated_local(request, tmpdir_factory):
    tmpdir = str(tmpdir_factory.mktemp('data'))
    _populate(tmpdir)
    return tmpdir


# This is the same directory structure as above but paramatrized on different file systems
@pytest.fixture(scope='session', params=inputs)
def populated(request, populated_local):
    if request.param == 'local':
        yield populated_local

    if request.param == 'gcs':
        path = os.path.join(BUCKET, 'data1')
        run('gsutil -m cp -r {} {}'.format(populated_local, path))
        yield path
        run('gsutil -m rm -r {}'.format(path))

    if request.param == 'gcs_extra':
        path = os.path.join(BUCKET, 'data2')
        run('gsutil -m cp -r {} {}'.format(populated_local, path))
        # Also add an extra file
        run('touch extra')
        run('gsutil cp extra {}'.format(path))
        os.remove('extra')
        yield path
        run('gsutil -m rm -r {}'.format(path))


@pytest.fixture(scope='session')
def keys():
    return pd.Series(['key{:02d}'.format(i) for i in range(40)])


@pytest.fixture()
def randomdata():
    df = pd.DataFrame(
        np.random.rand(10, 10),
        columns=['f{}'.format(i) for i in range(10)]
    )
    return df


@pytest.fixture()
def datadir_local(request, tmpdir_factory):
    return str(tmpdir_factory.mktemp('data'))


@pytest.fixture(params=outputs)
def datadir(request, tmpdir_factory):
    if request.param == 'local':
        tmpdir = str(tmpdir_factory.mktemp('data'))
        yield tmpdir

    if request.param == 'gcs':
        path = os.path.join(BUCKET, 'output')
        yield path
        run('gsutil -m rm -r {}'.format(path))


def _populate(tmpdir):
    """ Create a directory of blocks with 4 cgroups and 4 rgroups
    """
    for c in range(4):
        cgroup = os.path.join(tmpdir, 'c{}'.format(c))
        if not os.path.exists(cgroup):
            os.makedirs(cgroup)
        for r in range(4):
            df = pd.DataFrame(
                np.random.rand(10, 10),
                index=list(range(r*10, (r+1)*10)),
                columns=['f{}_{}'.format(c, i) for i in range(10)]
            )
            df['key'] = ['key{:02d}'.format(i) for i in df.index]  # common key for merges
            df.to_csv(os.path.join(cgroup, 'part.{}.csv'.format(r)), index=False)
