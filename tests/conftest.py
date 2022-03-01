import os
import pytest
import pandas as pd
import numpy as np
import uuid

from blocks.filesystem import FileSystem
from delegator import run

BUCKET_GCS = "gs://blocks-example"
BUCKET_S3 = "s3://blocks-example"

if os.environ.get("CI"):
    inputs = ["local"]
    outputs = ["local"]
    temps = ["local"]
else:
    inputs = ["local", "gcs", "gcs_extra", "s3"]
    outputs = ["local", "gcs", "s3"]
    temps = ["local", "gcs", "s3"]


@pytest.fixture
def fs(request):
    return FileSystem()


@pytest.fixture(scope="function", params=temps)
def temp(request, tmpdir_factory):
    if request.param == "local":
        path = str(tmpdir_factory.mktemp("temp"))
        yield path

    if request.param == "gcs":
        path = os.path.join(BUCKET_GCS, "temp")
        yield path
        run("gsutil rm -r {}".format(path))

    if request.param == "s3":
        path = os.path.join(BUCKET_S3, "temp")
        yield path
        run("aws s3 rm --recursive {}".format(path))


@pytest.fixture(scope="session")
def populated_local(request, tmpdir_factory):
    tmpdir = str(tmpdir_factory.mktemp("data"))
    _populate(tmpdir)
    return tmpdir


# This is the same directory structure as above but parametrized on different file systems
@pytest.fixture(scope="session", params=inputs)
def populated(request, populated_local):
    if request.param == "local":
        yield populated_local

    if request.param == "gcs":
        path = os.path.join(BUCKET_GCS, "data1")
        run("gsutil cp -r {} {}".format(populated_local, path))
        yield path
        run("gsutil rm -r {}".format(path))

    if request.param == "gcs_extra":
        path = os.path.join(BUCKET_GCS, "data2")
        run("gsutil cp -r {} {}".format(populated_local, path))
        # Also add an extra file
        run("touch extra")
        run("gsutil cp extra {}".format(path))
        os.remove("extra")
        yield path
        run("gsutil rm -r {}".format(path))

    if request.param == "s3":
        path = os.path.join(BUCKET_S3, "data1")
        run("aws s3 cp --recursive {} {}".format(populated_local, path))
        yield path
        run("aws s3 rm --recursive {}".format(path))


@pytest.fixture(scope="session")
def keys():
    return pd.Series(["key{:02d}".format(i) for i in range(40)])


@pytest.fixture()
def randomdata():
    df = pd.DataFrame(
        np.random.rand(10, 10), columns=["f{}".format(i) for i in range(10)]
    )
    return df


@pytest.fixture()
def datadir_local(request, tmpdir_factory):
    return str(tmpdir_factory.mktemp("data"))


@pytest.fixture(params=outputs)
def datadir(request, tmpdir_factory):
    output = str(uuid.uuid4()).replace("-", "")
    if request.param == "local":
        tmpdir = str(tmpdir_factory.mktemp("data"))
        yield tmpdir

    if request.param == "gcs":
        path = os.path.join(BUCKET_GCS, output)
        yield path
        run("gsutil rm -r {}".format(path))

    if request.param == "s3":
        path = os.path.join(BUCKET_S3, output)
        yield path
        run("aws s3 rm --recursive {}".format(path))


def _populate(tmpdir):
    """Create a directory of blocks with 4 cgroups and 4 rgroups"""
    for c in range(4):
        cgroup = os.path.join(tmpdir, "c{}".format(c))
        if not os.path.exists(cgroup):
            os.makedirs(cgroup)
        for r in range(4):
            df = pd.DataFrame(
                np.random.rand(10, 10),
                index=list(range(r * 10, (r + 1) * 10)),
                columns=["f{}_{}".format(c, i) for i in range(10)],
            )
            df["key"] = [
                "key{:02d}".format(i) for i in df.index
            ]  # common key for merges
            df.to_csv(os.path.join(cgroup, "part.{}.csv".format(r)), index=False)
