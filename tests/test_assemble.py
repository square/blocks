#!/usr/bin/env python
# -*- coding: utf-8 -*-
from blocks import core


def test_assemble(populated):
    df = core.assemble(populated)
    assert(df.shape == (40, 41))
    expected = ['f{}_{}'.format(i, j) for i in range(4) for j in range(10)]
    expected.append('key')
    assert(set(df.columns) == set(expected))


def test_assemble_ordered(populated):
    order = ['c2', 'c1', 'c3', 'c0']
    df = core.assemble(populated, cgroups=order)
    assert(df.shape == (40, 41))
    expected = ['f{}_{}'.format(order[i][1], j) for i in range(4) for j in range(10)]
    expected.append('key')
    assert(set(df.columns) == set(expected))
    # Check the features are in the right order
    assert([c for c in df.columns if c != 'key'] == expected[:-1])


# Various options do not depend on filesystem so we can just test locally
def test_assemble_filtered_cgroup(populated_local, keys):
    df = core.assemble(populated_local, cgroups=['c0', 'c3'])
    assert(df.shape == (40, 21))
    expected = ['f{}_{}'.format(i, j) for i in [0, 3] for j in range(10)]
    expected.append('key')
    assert(set(df.columns) == set(expected))
    assert((df.key == keys).all())


def test_assemble_filtered_rgroup(populated_local, keys):
    df = core.assemble(populated_local, rgroups=['part.0.csv', 'part.1.csv'])
    assert(df.shape == (20, 41))
    expected = ['f{}_{}'.format(i, j) for i in range(4) for j in range(10)]
    expected.append('key')
    assert(set(df.columns) == set(expected))
    assert((df.key == keys[:20].reset_index(drop=True)).all())


def test_assemble_read_args(populated_local, keys):
    read_args = {'dtype': str}
    df = core.assemble(populated_local, read_args=read_args)
    assert(df.shape == (40, 41))
    expected = ['f{}_{}'.format(i, j) for i in range(4) for j in range(10)]
    expected.append('key')
    assert(set(df.columns) == set(expected))
    assert((df.key == keys).all())
    assert((df.dtypes == 'object').all())


def test_assemble_cgroup_args(populated_local, keys):
    cgroup_args = {'c0': {'dtype': str}}
    df = core.assemble(populated_local, cgroup_args=cgroup_args)
    assert(df.shape == (40, 41))
    expected = ['f{}_{}'.format(i, j) for i in range(4) for j in range(10)]
    expected.append('key')
    assert(set(df.columns) == set(expected))
    assert((df.key == keys).all())
    for col in ['f0_{}'.format(i) for i in range(10)]:
        assert(df.dtypes[col] == 'object')
