#!/usr/bin/env python
# -*- coding: utf-8 -*-
from blocks import core


def test_iterate(populated):
    dfs = core.iterate(populated)
    for i in range(4):
        for j in range(4):
            cname, rname, df = next(dfs)
            assert(cname == 'c{}'.format(i))
            assert(rname == 'part.{}.csv'.format(j))
            assert(df.shape == (10, 11))


def test_iterate_ordered(populated):
    order = ['c3', 'c1', 'c2', 'c0']
    dfs = core.iterate(populated, cgroups=order)
    for i in range(4):
        for j in range(4):
            cname, rname, df = next(dfs)
            assert(cname == order[i])
            assert(rname == 'part.{}.csv'.format(j))
            assert(df.shape == (10, 11))


def test_iterate_axis0(populated):
    dfs = core.iterate(populated, axis=0)
    for i in range(4):
        rname, df = next(dfs)
        assert(rname == 'part.{}.csv'.format(i))
        assert(df.shape == (10, 41))


def test_iterate_axis1(populated):
    dfs = core.iterate(populated, axis=1)
    for i in range(4):
        cname, df = next(dfs)
        assert(cname == 'c{}'.format(i))
        assert(df.shape == (40, 11))
