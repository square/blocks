#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
from blocks import core


def test_expand(populated, fs):
    expected = [os.path.join(populated, 'c{}/part.{}.csv'.format(i, j))
                for i in xrange(4)
                for j in xrange(4)]

    # All of these patterns should expand into the same set of files
    for ex in ['', '*', '*/*', '**']:
        ex = os.path.join(populated, ex)
        paths = fs.ls(ex)
        expanded = core._expand(paths, fs)
        assert(expanded == expected)


def test_expand_pattern(populated, fs):
    expected = [os.path.join(populated, 'c{}/part.{}.csv'.format(i, j))
                for i in xrange(2)
                for j in xrange(4)]

    # All of these patterns should expand into the same set of files
    for ex in ['c[01]', 'c[01]/*']:
        ex = os.path.join(populated, ex)
        paths = fs.ls(ex)
        expanded = core._expand(paths, fs)
        assert(expanded == expected)


def test_cgroups():
    expanded = [
        'base/c{}/part.{}.csv'.format(i, j)
        for i in xrange(4)
        for j in xrange(4)
    ]
    cgroups = core._cgroups(expanded)
    for i in xrange(4):
        key = 'c{}'.format(i)
        assert(key in cgroups)
        assert(cgroups[key] == ['base/c{}/part.{}.csv'.format(i, j) for j in xrange(4)])


def test_access(populated, fs):
    paths = fs.ls(populated)
    expanded = core._expand(paths, fs)
    cgroups = core._cgroups(expanded)
    cgroups = core._access(cgroups, fs)
    assert(len(cgroups) == 4)
    for c, paths in cgroups.iteritems():
        assert(len(paths) == 4)
        for p in paths:
            assert(os.path.exists(p))
