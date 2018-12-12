#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os

TEST_STRING = 'test'


def test_ls_directory(populated, fs):
    ex = os.path.join(populated, '')
    expected = [os.path.join(populated, 'c{}'.format(i)) for i in xrange(4)]

    # one of the fixtures has an extra file
    found = fs.ls(ex)
    if len(found) > 4:
        found = found[:-1]
    assert(found == expected)


def test_ls_wildcard(populated, fs):
    ex = os.path.join(populated, '*/part.1.csv')
    expected = [os.path.join(populated, 'c{}/part.1.csv'.format(i)) for i in xrange(4)]
    assert(fs.ls(ex) == expected)


def test_ls_double_wildcard(populated, fs):
    ex = os.path.join(populated, '**')
    expected = {os.path.join(populated, 'c{}/part.{}.csv'.format(i, j))
                for i in xrange(4)
                for j in xrange(4)}
    assert(expected.issubset(set(fs.ls(ex))))


def test_ls_pattern(populated, fs):
    ex = os.path.join(populated, '*/part.[01].csv')
    expected = [os.path.join(populated, 'c{}/part.{}.csv'.format(i, j))
                for i in xrange(4)
                for j in xrange(2)]
    assert(fs.ls(ex) == expected)


def test_store_access(gcstemp, fs):
    paths = []
    with fs.store(gcstemp, ['ex1.txt', 'ex2.txt']) as datafiles:
        for d in datafiles:
            paths.append(d.path)
            d.handle.write(TEST_STRING)

    datafiles = fs.access(paths)
    for d in datafiles:
        assert(d.handle.read() == TEST_STRING)
