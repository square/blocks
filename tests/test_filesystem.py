#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
from uuid import uuid4

TEST_STRING = b'test'


def test_ls_directory(populated, fs):
    ex = os.path.join(populated, '')
    expected = [os.path.join(populated, 'c{}'.format(i)) for i in range(4)]

    # one of the fixtures has an extra file
    found = fs.ls(ex)
    if len(found) > 4:
        found = found[:-1]
    assert(found == expected)


def test_ls_wildcard(populated, fs):
    ex = os.path.join(populated, '*/part.1.csv')
    expected = [os.path.join(populated, 'c{}/part.1.csv'.format(i)) for i in range(4)]
    assert(fs.ls(ex) == expected)


def test_ls_double_wildcard(populated, fs):
    ex = os.path.join(populated, '**')
    expected = {os.path.join(populated, 'c{}/part.{}.csv'.format(i, j))
                for i in range(4)
                for j in range(4)}
    assert(expected.issubset(set(fs.ls(ex))))


def test_ls_pattern(populated, fs):
    ex = os.path.join(populated, '*/part.[01].csv')
    expected = [os.path.join(populated, 'c{}/part.{}.csv'.format(i, j))
                for i in range(4)
                for j in range(2)]
    assert(fs.ls(ex) == expected)


def test_open_read(populated, fs):
    with fs.open(os.path.join(populated, 'c0/part.0.csv'), 'r') as f:
        assert(f.readline() == 'f0_0,f0_1,f0_2,f0_3,f0_4,f0_5,f0_6,f0_7,f0_8,f0_9,key\n')


def test_open_write(temp, fs):
    content = str(uuid4())
    path = os.path.join(temp, 'content')
    with fs.open(path, 'w') as f:
        f.write(content)

    with fs.open(path, 'r') as f:
        assert(f.read() == content)


def test_store_access(temp, fs):
    paths = []
    with fs.store(temp, ['ex1.txt', 'ex2.txt']) as datafiles:
        for d in datafiles:
            paths.append(d.path)
            d.handle.write(TEST_STRING)

    datafiles = fs.access(paths)
    for d in datafiles:
        assert(d.handle.read() == TEST_STRING)


def test_copy_recursive_to_local(populated, tmpdir, fs):
    dest = str(tmpdir)
    fs.cp(populated, dest, recursive=True)
    source = [p.replace(populated, '') for p in fs.ls(populated + '/**')]
    copy = fs.ls(dest + '/**')
    assert(s in c for s, c in zip(source, copy))


def test_copy_recursive_matched(populated, fs):
    dest = populated.replace('data', 'copy')
    fs.cp(populated, dest, recursive=True)
    source = [p.replace(populated, '') for p in fs.ls(populated + '/**')]
    copy = fs.ls(dest + '/**')
    assert(s in c for s, c in zip(source, copy))


def test_rm(populated, fs):
    dest = populated.replace('data', 'copy')
    fs.cp([os.path.join(populated, '')], dest, recursive=True)
    assert(fs.ls(dest))
    fs.rm([dest], recursive=True)
    assert(fs.ls(dest) == [])
