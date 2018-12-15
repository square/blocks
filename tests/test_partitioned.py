#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pytest
from blocks import core


def test_partitioned(populated):
    pytest.importorskip('dask.dataframe')
    df = core.partitioned(populated).compute()
    assert(df.shape == (40, 41))
    expected = ['f{}_{}'.format(i, j) for i in range(4) for j in range(10)]
    expected.append('key')
    assert(set(df.columns) == set(expected))
