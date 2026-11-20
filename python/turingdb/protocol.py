from typing import Protocol

from pandas import DataFrame


# Maps server-side type names to pandas dtypes. Anything not listed falls
# back to object dtype on the consumer side. Used by BinaryClient and
# EmbeddedClient (over the dtypes dict emitted from the nanobind bindings).
DTYPE_MAP = {
    "String": "string",
    "Int64": "Int64",
    "UInt8": "UInt8",
    "UInt16": "UInt16",
    "UInt32": "UInt32",
    "UInt64": "UInt64",
    "Double": "Float64",
    "Bool": "boolean",
    "DateTime": "datetime64[us, UTC]",
    "Duration": "timedelta64[us]",
}


def to_series(values, dtype: str):
    """A column of query_raw() as a pandas Series of the given dtype.

    A nullable number column arrives as a numpy masked array, its mask True
    where a row is null. It becomes a pandas masked array without a copy. An
    ID column keeps the width of the engine's ID, so a PropertyTypeID is
    UInt16. A double column is Float64, nullable or not: a null is pd.NA and
    a NaN value stays NaN. Any other column is a numpy array, kept without a
    copy, or a list from the embedded engine; StringArray checks a string
    array in place where pandas stores strings as Python objects.
    """
    import numpy as np
    import pandas as pd

    if not isinstance(values, np.ma.MaskedArray):
        if not isinstance(values, np.ndarray):
            return pd.Series(values, dtype=dtype)
        elif dtype == "string" and pd.StringDtype().storage == "python":
            return pd.Series(pd.arrays.StringArray(values, copy=False), copy=False)
        elif dtype == "Float64":
            # pandas would read a NaN as a null and copy the values to mask it
            no_nulls = np.zeros(len(values), dtype=np.bool_)
            return pd.Series(pd.arrays.FloatingArray(values, no_nulls, copy=False), copy=False)
        return pd.Series(values, dtype=dtype, copy=False)

    data = values.data
    mask = np.ma.getmaskarray(values)
    if dtype in ("Int64", "UInt8", "UInt16", "UInt32", "UInt64"):
        return pd.Series(pd.arrays.IntegerArray(data, mask, copy=False), copy=False)
    elif dtype == "boolean":
        return pd.Series(pd.arrays.BooleanArray(data.view(np.bool_), mask, copy=False), copy=False)
    elif dtype == "Float64":
        return pd.Series(pd.arrays.FloatingArray(data, mask, copy=False), copy=False)
    else:
        return pd.Series(values.astype(object).filled(None), dtype=dtype, copy=False)


def to_dataframe(raw: dict) -> DataFrame:
    """The result of query_raw() as a DataFrame, each column of its DTYPE_MAP dtype."""
    import pandas as pd

    dtypes = raw["dtypes"]
    return pd.DataFrame(
        {
            col: to_series(values, DTYPE_MAP.get(dtypes[col], "object"))
            for col, values in raw["data"].items()
        },
        copy=False,
    )


class QueryProtocol(Protocol):
    def query(self, query: str) -> DataFrame: ...
