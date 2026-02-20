#   Copyright (c) 2024-2026 Anton Kundenko <singaraiona@gmail.com>
#   All rights reserved.
#
#   Permission is hereby granted, free of charge, to any person obtaining a copy
#   of this software and associated documentation files (the "Software"), to deal
#   in the Software without restriction, including without limitation the rights
#   to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#   copies of the Software, and to permit persons to whom the Software is
#   furnished to do so, subject to the following conditions:
#
#   The above copyright notice and this permission notice shall be included in all
#   copies or substantial portions of the Software.
#
#   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#   IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#   FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#   AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#   LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#   OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#   SOFTWARE.

"""
Teide Python API integration tests.

Tests the high-level API (Context, Table, Query, Expr, GroupBy, Series)
using a small synthetic CSV. Run with:

    TEIDE_LIB=build_release/libteide.so python -m pytest test/test_api.py -v
"""

import os
import sys
import tempfile
import pytest

# Ensure bindings are importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "py"))

from teide.api import Context, Table, Query, Expr, GroupBy, Series, col, lit


@pytest.fixture(scope="module")
def csv_path():
    """Create a small test CSV file."""
    data = (
        "id1,id2,v1,v2,v3\n"
        "a,x,1,10,1.5\n"
        "a,y,2,20,2.5\n"
        "b,x,3,30,3.5\n"
        "b,y,4,40,4.5\n"
        "c,x,5,50,5.5\n"
        "a,x,6,60,6.5\n"
        "b,y,7,70,7.5\n"
        "c,y,8,80,8.5\n"
        "a,y,9,90,9.5\n"
        "c,x,10,100,10.5\n"
    )
    fd, path = tempfile.mkstemp(suffix=".csv")
    with os.fdopen(fd, "w") as f:
        f.write(data)
    yield path
    os.unlink(path)


@pytest.fixture(scope="module")
def ctx(csv_path):
    """Create a Context and load the test CSV."""
    with Context() as c:
        yield c


@pytest.fixture(scope="module")
def table(ctx, csv_path):
    """Load the test CSV into a Table."""
    return ctx.read_csv(csv_path)


class TestContext:
    def test_context_manager(self, csv_path):
        with Context() as ctx:
            tbl = ctx.read_csv(csv_path)
            assert tbl is not None
            assert len(tbl) == 10

    def test_read_csv(self, table):
        assert table is not None
        assert len(table) == 10


class TestTable:
    def test_shape(self, table):
        rows, cols = table.shape
        assert rows == 10
        assert cols == 5

    def test_columns(self, table):
        cols = table.columns
        assert len(cols) == 5
        assert "id1" in cols
        assert "v1" in cols
        assert "v3" in cols

    def test_getitem(self, table):
        s = table["v1"]
        assert isinstance(s, Series)
        assert len(s) == 10

    def test_getitem_missing(self, table):
        with pytest.raises(KeyError):
            table["nonexistent"]

    def test_head(self, table):
        h = table.head(3)
        assert len(h) == 3

    def test_to_dict(self, table):
        d = table.to_dict()
        assert isinstance(d, dict)
        assert "v1" in d
        assert len(d["v1"]) == 10

    def test_repr(self, table):
        r = repr(table)
        assert "10 rows" in r
        assert "5 cols" in r


class TestSeries:
    def test_len(self, table):
        s = table["v1"]
        assert len(s) == 10

    def test_to_list_i64(self, table):
        vals = table["v1"].to_list()
        assert len(vals) == 10
        assert vals[0] == 1
        assert vals[-1] == 10

    def test_to_list_f64(self, table):
        vals = table["v3"].to_list()
        assert len(vals) == 10
        assert abs(vals[0] - 1.5) < 1e-10
        assert abs(vals[-1] - 10.5) < 1e-10

    def test_to_list_enum(self, table):
        vals = table["id1"].to_list()
        assert len(vals) == 10
        assert vals[0] == "a"
        assert vals[2] == "b"
        assert vals[4] == "c"


class TestExpr:
    def test_col(self):
        e = col("x")
        assert e.kind == "col"
        assert e.kw["name"] == "x"

    def test_lit(self):
        e = lit(42)
        assert e.kind == "lit"
        assert e.kw["value"] == 42

    def test_arithmetic(self):
        e = col("x") + lit(1)
        assert e.kind == "binop"
        assert e.kw["op"] == "add"

    def test_comparison(self):
        e = col("x") > lit(0)
        assert e.kind == "binop"
        assert e.kw["op"] == "gt"

    def test_agg(self):
        e = col("x").sum()
        assert e.kind == "agg"

    def test_chain(self):
        e = (col("x") + col("y")) * lit(2)
        assert e.kind == "binop"
        assert e.kw["op"] == "mul"


class TestQuery:
    def test_group_sum(self, table):
        """Q1: sum(v1) group by id1"""
        result = (
            table.group_by("id1")
                 .agg(col("v1").sum())
                 .collect()
        )
        assert result is not None
        rows, cols = result.shape
        assert rows == 3  # a, b, c
        assert cols == 2  # id1, v1_sum

    def test_group_multi_key(self, table):
        """Q2: sum(v1) group by id1, id2"""
        result = (
            table.group_by("id1", "id2")
                 .agg(col("v1").sum())
                 .collect()
        )
        assert result is not None
        rows, _ = result.shape
        # a-x, a-y, b-x, b-y, c-x, c-y = 6 groups
        assert rows == 6

    def test_group_multi_agg(self, table):
        """Q3: sum(v1), mean(v3) group by id1"""
        result = (
            table.group_by("id1")
                 .agg(col("v1").sum(), col("v3").mean())
                 .collect()
        )
        assert result is not None
        rows, cols = result.shape
        assert rows == 3
        assert cols == 3  # id1, sum_v1, mean_v3

    def test_sort(self, table):
        """Sort by v1 descending."""
        result = (
            table.sort("v1", descending=True)
                 .collect()
        )
        assert result is not None
        assert len(result) == 10
