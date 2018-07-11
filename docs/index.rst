======
Blocks
======

.. image:: blocks.gif

*blocks* provides a simple interface to read, organize, and manipulate structured data on disk

.. code-block:: python

    import blocks

    df = blocks.assemble('data/*.csv')
    train = blocks.assemble('data/*[01].csv')
    test = blocks.assemble('data/*[2-9].csv')


The interface emulates the tools you're used to from the command line, with full support for globbing and pattern
matching. And *blocks* can handle more complicated structures as your data grows in complexity:

=======================  =====================================================================
Layout                   Recipe
=======================  =====================================================================
.. image:: both.png      .. code-block:: python

                             blocks.assemble('data/')``

.. image:: column.png    .. code-block:: python

                             blocks.assemble('data/g1/*')

.. image:: row.png       .. code-block:: python

                             blocks.assemble('data/*/part_01.pq')

.. image:: filtered.png  .. code-block:: python

                             blocks.assemble('data/*/part_01.pq', cgroups=['g0', 'g1', 'g3'])

=======================  =====================================================================

Blocks supports data on your local computer but also data stored on GCS, assuming you have already setup the `Google Cloud SDK`_


Full Contents
-------------

.. toctree::

   quickstart
   examples
   core
   filesystem


.. _Google Cloud SDK: https://cloud.google.com/sdk/docs/
