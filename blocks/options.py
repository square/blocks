import asyncio
import numpy as np
import os
import pandas as pd
import fsspec
import time
from functools import partial
from concurrent.futures import ProcessPoolExecutor

fs = fsspec.get_filesystem_class("gs")()


def fake(n, m, count):
    paths = []
    for i in range(count):
        df = pd.DataFrame(np.random.rand(n, m), columns=[str(j) for j in range(m)])
        path = f"test{i}.pq"
        paths.append(path)
        df.to_parquet(path)

    fs.put(paths, "gs://sq-ds-capital-prod/test/parts/")
    return [f"gs://sq-ds-capital-prod/test/parts/test{i}.pq" for i in range(count)]


n = 1000000
m = 100
count = 10

print(f"Fake data with {count} ({n},{m})")
paths = fake(n, m, count)


def get_and_read(paths):
    # Copy to local files
    local = [os.path.basename(p) for p in paths]
    print(paths, local)
    fs.get(paths, local)

    dfs = []
    for fname in local:
        dfs.append(pd.read_parquet(fname))
        os.remove(fname)

    return pd.concat(dfs, axis=0)


async def async_get_and_read(paths):
    local = ["tmp/" + os.path.basename(p) for p in paths]
    loop = asyncio.get_running_loop()
    fs = fsspec.get_filesystem_class("gs")(asynchronous=True, loop=loop)
    await fs.set_session()
    await asyncio.gather(*[fs._get_file(p, l) for p, l in zip(paths, local)])
    await fs.session.close()

    with ProcessPoolExecutor() as pool:
        dfs = await asyncio.gather(
            *[
                loop.run_in_executor(pool, partial(pd.read_parquet, fname))
                for fname in local
            ]
        )
    return pd.concat(dfs, axis=0)


start = time.time()
df = get_and_read(paths)
print(f"get_and_read finished in {time.time() - start} seconds")

start = time.time()
df = asyncio.run(async_get_and_read(paths))
print(f"async_get_and_read finished in {time.time() - start} seconds")
