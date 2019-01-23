# -*- coding: utf-8 -*-
import os
import numpy as np
import pandas as pd
import warnings

from functools import reduce
from collections import defaultdict, OrderedDict
from blocks.dfio import read_df, write_df
from blocks.filesystem import GCSFileSystem


def assemble(path, cgroups=None, rgroups=None,
             read_args={}, cgroup_args={}, merge='inner', filesystem=GCSFileSystem()):
    """ Assemble multiple dataframe blocks into a single frame

    Each file included in the path (or subdirs of that path) is combined into
    a single dataframe by first concatenating over row groups and then merging
    over cgroups. The merges are performed in the order of listed cgroups if
    provided, otherwise in alphabetic order. Files are opened by a method inferred
    from their extension

    Parameters
    ----------
    path : str
        The glob-able path to all datafiles to assemble into a frame
        e.g. gs://example/*/*, gs://example/*/part.0.pq, gs://example/c[1-2]/*
        See the README for a more detailed explanation
    cgroups : list of str, optional
        The list of cgroups (folder names) to include from the glob path
    rgroups : list of str, optional
        The list of rgroups (file names) to include from the glob path
    read_args : optional
        Any additional keyword args to pass to the read function
    cgroup_args : {cgroup: kwargs}, optional
        Any cgroup specific read arguments, where each key is the name
        of the cgroup and each value is a dictionary of keyword args
    merge : one of 'left', 'right', 'outer', 'inner', default 'inner'
        The merge strategy to pass to pandas.merge
    filesystem : blocks.filesystem.FileSystem or similar
        A filesystem object that implements the blocks.FileSystem API

    Returns
    -------
    data : pd.DataFrame
        The combined dataframe from all the blocks

    """
    grouped = _collect(path, cgroups, rgroups, filesystem)

    # ----------------------------------------
    # Concatenate all rgroups
    # ----------------------------------------
    frames = []

    for group in grouped:
        datafiles = grouped[group]
        args = read_args.copy()
        if group in cgroup_args:
            args.update(cgroup_args[group])
        frames.append(pd.concat(read_df(d, **args) for d in datafiles))

    # ----------------------------------------
    # Merge all cgroups
    # ----------------------------------------
    df = _merge_all(frames, merge=merge)

    # ----------------------------------------
    # Delete temporary files
    # ----------------------------------------
    for file in datafiles:
        if hasattr(file.handle, 'name'):
            tmp_file_path = file.handle.name
            if os.path.exists(tmp_file_path):
                os.remove(file.handle.name)
    return df


def iterate(path, axis=-1, cgroups=None, rgroups=None,
            read_args={}, cgroup_args={}, merge='inner', filesystem=GCSFileSystem()):
    """ Iterate over dataframe blocks

    Each file include in the path (or subdirs of that path) is opened as a
    dataframe and returned in a generator of (cname, rname, dataframe).
    Files are opened by a method inferred from their extension

    Parameters
    ----------
    path : str
        The glob-able path to all datafiles to assemble into a frame
        e.g. gs://example/*/*, gs://example/*/part.0.pq, gs://example/c[1-2]/*
        See the README for a more detailed explanation
    axis : int, default -1
        The axis to iterate along
        If -1 (the default), iterate over both columns and rows
        If 0, iterate over the rgroups, combining any cgroups
        If 1, iterate over the cgroups, combining any rgroups
    cgroups : list of str, or {str: args} optional
        The list of cgroups (folder names) to include from the glob path
    rgroups : list of str, optional
        The list of rgroups (file names) to include from the glob path
    read_args : dict, optional
        Any additional keyword args to pass to the read function
    cgroup_args : {cgroup: kwargs}, optional
        Any cgroup specific read arguments, where each key is the name
        of the cgroup and each value is a dictionary of keyword args
    merge : one of 'left', 'right', 'outer', 'inner', default 'inner'
        The merge strategy to pass to pandas.merge, only used when axis=0
    filesystem : blocks.filesystem.FileSystem or similar
        A filesystem object that implements the blocks.FileSystem API

    Returns
    -------
    data : generator
        A generator of (cname, rname, dataframe) for each collected path
        If axis=0, yields (rname, dataframe)
        If axis=1, yields (cname, dataframe)

    """
    grouped = _collect(path, cgroups, rgroups, filesystem)

    if axis == -1:
        for cgroup in grouped:
            args = read_args.copy()
            if cgroup in cgroup_args:
                args.update(cgroup_args[cgroup])
            for datafile in grouped[cgroup]:
                yield _cname(datafile.path), _rname(datafile.path), read_df(datafile, **args)

    elif axis == 0:
        # find the shared files among all subfolders
        rgroups = _shared_rgroups(grouped)

        for rgroup in sorted(rgroups):
            frames = []
            for cgroup in grouped:
                datafile = next(
                    d for d in grouped[cgroup]
                    if os.path.basename(d.path) == rgroup
                )

                args = read_args.copy()
                if cgroup in cgroup_args:
                    args.update(cgroup_args[cgroup])
                frames.append(read_df(datafile, **args))
            yield rgroup, _merge_all(frames, merge=merge)

    elif axis == 1:
        for cgroup in grouped:
            datafiles = grouped[cgroup]
            args = read_args.copy()
            if cgroup in cgroup_args:
                args.update(cgroup_args[cgroup])
            yield cgroup, pd.concat(read_df(d, **args) for d in datafiles)

    else:
        raise ValueError('Invalid choice for axis, options are -1, 0, 1')


def partitioned(path, cgroups=None, rgroups=None,
                read_args={}, cgroup_args={}, merge='inner', filesystem=GCSFileSystem()):
    """ Return a partitioned dask dataframe, where each partition is a row group

    The results are the same as iterate with axis=0, except that it returns a dask dataframe
    instead of a generator. Note that this requires dask to be installed

    Parameters
    ----------
    path : str
        The glob-able path to all datafiles to assemble into a frame
        e.g. gs://example/*/*, gs://example/*/part.0.pq, gs://example/c[1-2]/*
        See the README for a more detailed explanation
    cgroups : list of str, or {str: args} optional
        The list of cgroups (folder names) to include from the glob path
    rgroups : list of str, optional
        The list of rgroups (file names) to include from the glob path
    read_args : dict, optional
        Any additional keyword args to pass to the read function
    cgroup_args : {cgroup: kwargs}, optional
        Any cgroup specific read arguments, where each key is the name
        of the cgroup and each value is a dictionary of keyword args
    merge : one of 'left', 'right', 'outer', 'inner', default 'inner'
        The merge strategy to pass to pandas.merge, only used when axis=0
    filesystem : blocks.filesystem.FileSystem or similar
        A filesystem object that implements the blocks.FileSystem API

    Returns
    -------
    data : dask.dataframe
        A dask dataframe partitioned by row groups, with all cgroups merged

    """
    try:
        import dask
        import dask.dataframe as dd
    except ImportError:
        raise ImportError('Partitioned requires dask[dataframe] to be installed')

    grouped = _collect(path, cgroups, rgroups, filesystem)
    blocks = []

    @dask.delayed()
    def merged(rgroup):
        frames = []
        for cgroup in grouped:
            datafile = next(
                d for d in grouped[cgroup]
                if os.path.basename(d.path) == rgroup
            )
            args = read_args.copy()
            if cgroup in cgroup_args:
                args.update(cgroup_args[cgroup])
            frames.append(read_df(datafile,  **args))
        return _merge_all(frames, merge=merge)

    for rgroup in _shared_rgroups(grouped):
        blocks.append(merged(rgroup))
    return dd.from_delayed(blocks)


def place(df, path, filesystem=GCSFileSystem(), **write_args):
    """ Place a dataframe block onto the filesystem at the specified path

    Parameters
    ----------
    df : pd.DataFrame
        The data to place
    path : str
        Path to the directory (possibly on GCS) in which to place the columns
    write_args : dict
        Any additional args to pass to the write function
    filesystem : blocks.filesystem.FileSystem or similar
        A filesystem object that implements the blocks.FileSystem API

    """
    bucket, fname = os.path.dirname(path), os.path.basename(path)
    with filesystem.store(bucket, [fname]) as datafiles:
        write_df(df, datafiles[0], **write_args)


def divide(
        df, path,
        n_rgroup=1,
        rgroup_offset=0,
        cgroup_columns=None,
        extension='.pq',
        convert=False,
        filesystem=GCSFileSystem(),
        **write_args
):
    """ Split a dataframe into rgroups/cgroups and save to disk

    Note that this splitting does not preserve the original index, so make sure
    to have another column to track values

    Parameters
    ----------
    df : pd.DataFrame
        The data to divide
    path : str
        Path to the directory (possibly on GCS) in which to place the columns
    n_rgroup : int, default 1
        The number of row groups to partition the data into
        The rgroups will have approximately equal sizes
    rgroup_offset : int, default 0
        The index to start from in the name of file parts
        e.g. If rgroup_offset=10 then the first file will be `part_00010.pq`
    cgroup_columns : {cgroup: list of column names}
        The column lists to form cgroups; if None, do not make cgroups
        Each key is the name of the cgroup, and each value is the list of columns to include
        To reassemble later make sure to include join keys for each cgroup
    extension : str, default .pq
        The file extension for the dataframe (file type inferred from this extension
    convert : bool, default False
        If true attempt to coerce types to numeric. This can avoid issues with amiguous
        object columns but requires additional time
    filesystem : blocks.filesystem.FileSystem or similar
        A filesystem object that implements the blocks.FileSystem API
    write_args : dict
        Any additional args to pass to the write function

    """
    # Use a single dummy cgroup if None wanted
    if cgroup_columns is None:
        cgroup_columns = {None: df.columns}

    # Add leading dot if not in extension
    if extension[0] != '.':
        extension = '.'+extension

    if convert:
        for col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='ignore')

    for cname, columns in cgroup_columns.items():
        cgroup = df[columns]

        bucket = os.path.join(path, cname) if cname else path
        rnames = ['part_{:05d}{}'.format(i+rgroup_offset, extension) for i in range(n_rgroup)]
        with filesystem.store(bucket, rnames) as datafiles:
            for rgroup, d in zip(np.array_split(cgroup, n_rgroup), datafiles):
                write_df(rgroup.reset_index(drop=True), d, **write_args)


def _collect(path, cgroups, rgroups, filesystem):
    """ Collect paths into cgroups and download any gcs files for local access

    Parameters
    ----------
    path : str
        The glob-able path to all datafiles to assemble into a frame
        e.g. gs://example/*/*, gs://example/*/part.0.pq, gs://example/c[1-2]/*
        See the README for a more detailed explanation
    cgroups : list of str, optional
        The list of cgroups (folder names) to include from the glob path
    rgroups : list of str, optional
        The list of rgroups (file names) to include from the glob path
    filesystem : blocks.filesystem.FileSystem or similar
        A filesystem object that implements the blocks.FileSystem API

    Returns
    -------
    grouped : {cgroup: list of paths}
        Paths to local copies of the data, grouped by cgroup

    """
    # ----------------------------------------
    # Collect paths into cgroups
    # ----------------------------------------
    paths = filesystem.ls(path)
    if not paths:
        raise ValueError('Did not find any files at the path: {}'.format(path))
    expanded = _expand(paths, filesystem)
    filtered = _filter(expanded, cgroups, rgroups)
    grouped = _cgroups(filtered)
    accessed = _access(grouped, filesystem)

    # Go in specified cgroup order, or alphabetical if not specified
    if cgroups is None:
        cgroups = sorted(grouped.keys())

    return OrderedDict((k, accessed[k]) for k in cgroups)


def _has_ext(path):
    return os.path.splitext(path)[1] != ''


def _expand(paths, filesystem):
    """ For any directories in paths, expand into all the contained files
    """
    expanded = []
    for path in paths:
        if _has_ext(path):
            # Has an extension so treat it as a file
            expanded.append(path)
        else:
            # Otherwise try to read it like a directory
            expanded += filesystem.ls(os.path.join(path, '**'))
    return [p for p in expanded if _has_ext(p)]


def _filter(paths, cgroups, rgroups):
    """ Keep only paths with the appropriate cgroups and/or rgroups
    """
    kept = []
    for path in paths:
        valid_cgroup = (cgroups is None or _cname(path) in cgroups)
        valid_rgroup = (rgroups is None or _rname(path) in rgroups)
        if valid_cgroup and valid_rgroup:
            kept.append(path)
    return kept


def _base(path):
    """ Get base from path (name of the top level folder)
    """
    return os.path.dirname(os.path.dirname(path))


def _cname(path):
    """ Get cname from path (name of the parent folder)
    """
    return os.path.basename(os.path.dirname(path))


def _rname(path):
    """ Get cname from path (name of the file)
    """
    return os.path.basename(path)


def _cgroups(paths):
    """ Group paths by cgroup (the parent folder)
    """
    cgroups = defaultdict(list)
    for path in paths:
        cgroups[_cname(path)].append(path)
    return cgroups


def _access(cgroups, filesystem):
    """ Access potentially cloud stored files, preserving cgroups
    """
    updated = {}
    for cgroup, paths in cgroups.items():
        updated[cgroup] = filesystem.access(paths)
    return updated


def _safe_merge(df1, df2, merge='inner'):
    """ Merge two dataframes, warning of any shape differences
    """
    s1, s2 = df1.shape[0], df2.shape[0]
    if s1 != s2:
        warnings.warn('The two cgroups have a different number of rows: {} {}'.format(s1, s2))
    return pd.merge(df1, df2, how=merge)


def _merge_all(frames, merge='inner'):
    """ Merge a list of dataframes with safe merge
    """
    result = frames[0]
    for frame in frames[1:]:
        result = _safe_merge(result, frame, merge)
    return result


def _shared_rgroups(grouped):
    rgroups = [
        [_rname(d.path) for d in group]
        for group in grouped.values()
    ]
    return reduce(lambda a, b: set(a) & set(b), rgroups)
