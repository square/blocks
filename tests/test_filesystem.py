#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os


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


def test_copy_single(populated, fs, tmpdir):
    local = str(tmpdir)

    source = [os.path.join(populated, 'c0/part.1.csv')]
    fs.copy(source, local)

    expected = [os.path.join(local, 'part.1.csv')]
    assert(fs.ls(os.path.join(local, '**')) == expected)


def test_copy_multiple(populated, fs, tmpdir):
    local = str(tmpdir)

    source = [os.path.join(populated, 'c0/part.1.csv'), os.path.join(populated, 'c0/part.2.csv')]
    fs.copy(source, local)

    expected = [os.path.join(local, 'part.1.csv'), os.path.join(local, 'part.2.csv')]
    assert(fs.ls(os.path.join(local, '**')) == expected)


def test_copy_single_dir(populated, fs, tmpdir):
    local = str(tmpdir)

    source = [os.path.join(populated, 'c0')]
    fs.copy(source, local)

    expected = [os.path.join(local, 'c0/part.{}.csv'.format(i)) for i in xrange(4)]
    assert(fs.ls(os.path.join(local, '**')) == expected)


def test_copy_multiple_dir(populated, fs, tmpdir):
    local = str(tmpdir)

    source = [os.path.join(populated, 'c0'), os.path.join(populated, 'c1')]
    fs.copy(source, local)

    expected = [os.path.join(local, 'c{}/part.{}.csv'.format(i, j))
                for i in xrange(2)
                for j in xrange(4)]
    assert(fs.ls(os.path.join(local, '**')) == expected)
