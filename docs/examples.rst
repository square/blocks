========
Examples
========

Inspect Data
------------

You can use assemble to grab a small subset of your data

.. code-block:: python

    import blocks

    df = blocks.assemble('data/*/part_00.pq')
    df.describe()


This works great when dealing with data staged on GCS

.. code-block:: python

    import blocks

    df = blocks.assemble('gs://bucket/*/part_00.pq')
    df.describe()


Large Datasets
--------------

It's common to end up with a dataset that won't easily fit into memory. But you often still need to calculate
aggregate statistics on that data. For example, you might need to get a unique list of categories in one of your fields.

Iterate makes this easy:

.. code-block:: python

    import blocks

    uniques = set()
    for _, _, block in blocks.iterate('data/'):
        uniques |= set(block['feature'])


or maybe you want to parallelize the process

.. code-block:: python

    import blocks
    from multiprocessing import Pool

    def unique_f1(block):
        return set(block[-1]['feature'])

    uniques_per_block = Pool(4).map(unique_f1, blocks.iterate('data/'))
    uniques = reduce(lambda a, b: a | b, uniques_per_block)


Of course if you have dask installed this is even easier

.. code-block:: python

    import blocks

    uniques = blocks.partitioned('data')['feature'].unique().compute()


Batch Training
--------------

If you're working with a tool like Keras, you might want to train a model on an iterator of batches
without every loading more than one partition into memory:

.. code-block:: python

    import blocks

    def batch_generator(path):
        for _, df in blocks.iterate(path, axis=0):
            while df.shape[0] >= nbatch:
                # Grab a sample and drop from original
                sub = df.sample(nbatch)
                df.drop(sub.index, inplace=True)
                yield sub.values

    model.fit_generator(
        generator=batch_generator('train/'),
        validation_data=batch_generator('validate/'),
    )

If you use an efficient file format like ``parquet``, this simple code will be suprisingly fast. You should make
sure that you don't use multiple cgroups in a situation like this, however, because merging can slow
down the process.


Combining
---------

If you end up with a dataset with multiple column groups, say because you grabbed your data from multiple sources,
you may want to merge accross those groups. However it is expensive to do this by loading the whole dataset into memory.
If you use the blocks structure you can merge each row partition separately and then save to new files. You can
even subdivide those files into smaller row groups to ensure that they don't grow too large:


.. code-block:: python

    import blocks

    offset = 0
    for _, df in blocks.iterate(path, axis=0):
        blocks.divide(df, 'combined/', n_rgroup=10, rgroup_offset=offset)
        rgroup_offset += 10


Filesystem
----------

Blocks provide a default filesystem that supports local files and GCS files. If you need additional functionality,
you can create a custom filesystem instance:


.. code-block:: python

    import blocks
    from blocks.filesystem import GCSFileSystem

    # disable parallel file copies
    # this is usually slower but can save some memory use
    fs = GCSFileSystem(parallel=False)

    df = blocks.assemble('gs://bucket/data/', filesystem=fs)


The default filesystem has a few options (in the API docs), but more importantly you can implement your own FileSystem
class by inheriting from ``blocks.filesystem.FileSystem``. This can be used to extend blocks to additional cloud platforms,
to support encryption/decryption, etc...
