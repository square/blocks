#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
from blocks import core


def test_expand(populated, fs):
    expected = [
        os.path.join(populated, "c{}/part.{}.csv".format(i, j))
        for i in range(4)
        for j in range(4)
    ]

    # All of these patterns should expand into the same set of files
    for ex in ["", "*/*", "**"]:
        ex = os.path.join(populated, ex)
        paths = fs.ls(ex)
        expanded = sorted(core._expand(paths, fs))
        assert expanded == expected


def test_expand_pattern(populated, fs):
    expected = [
        os.path.join(populated, "c{}/part.{}.csv".format(i, j))
        for i in range(2)
        for j in range(4)
    ]

    # All of these patterns should expand into the same set of files
    for ex in ["c[01]**", "c[01]/*"]:
        ex = os.path.join(populated, ex)
        paths = fs.ls(ex)
        expanded = sorted(core._expand(paths, fs))
        assert expanded == expected


def test_cgroups():
    expanded = ["base/c{}/part.{}.csv".format(i, j) for i in range(4) for j in range(4)]
    cgroups = core._cgroups(expanded)
    for i in range(4):
        key = "c{}".format(i)
        assert key in cgroups
        assert cgroups[key] == ["base/c{}/part.{}.csv".format(i, j) for j in range(4)]


def test_access(populated, fs, tmpdir):
    tmpdir = str(tmpdir)
    paths = fs.ls(populated)
    expanded = core._expand(paths, fs)
    cgroups = core._cgroups(expanded)
    cgroups = core._access(cgroups, fs, tmpdir)
    assert len(cgroups) == 4
    for c, paths in cgroups.items():
        assert len(paths) == 4
        for path in paths:
            with open(path, "r") as f:
                assert f.read()
