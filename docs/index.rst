======
Blocks
======

.. image:: blocks.gif

Blocks provides a simple interface to read, organize, and manipulate structured data in files
on local and cloud storage

Install
-------------
.. code-block:: bash

    pip install sq-blocks

Features
--------

.. code-block:: python

    import blocks
  
    # Load one or more files with the same interface
    df = blocks.assemble('data.csv')
    train = blocks.assemble('data/*[0-7].csv')
    test = blocks.assemble('data/*[89].csv')
  
    # With direct support for files on GCS
    df = blocks.assemble('gs://mybucket/data.csv')
    df = blocks.assemble('gs://mybucket/data/*.csv')
                
The interface emulates the tools you're used to from the command line, with full support for globbing and pattern
matching. And blocks can handle more complicated structures as your data grows in complexity:

=======================  =====================================================================
Layout                   Recipe
=======================  =====================================================================
.. image:: both.png      .. code-block:: python

                             blocks.assemble('data/**')

.. image:: column.png    .. code-block:: python

                             blocks.assemble('data/g1/*')

.. image:: row.png       .. code-block:: python

                             blocks.assemble('data/*/part_01.pq')

.. image:: filtered.png  .. code-block:: python

                             blocks.assemble('data/g[124]/part_01.pq')

=======================  =====================================================================



.. toctree::
   :hidden:

   quickstart
   examples
   core
   filesystem

