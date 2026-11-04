from typing import Protocol

from pandas import DataFrame


# The port a TuringDB server listens on unless started with -p
DEFAULT_PORT = 6686
DEFAULT_HOST = f"http://localhost:{DEFAULT_PORT}"

# Maps server-side type names to pandas dtypes. Anything not listed falls
# back to object dtype on the consumer side. Used by both HTTPClient (over
# the JSON header.column_types channel) and BinaryClient (over the dtypes
# dict emitted from the nanobind bindings).
DTYPE_MAP = {
    "String": "string",
    "Int64": "Int64",
    "UInt64": "UInt64",
    "Double": "float64",
    "Bool": "boolean",
    "DateTime": "datetime64[us, UTC]",
    "Duration": "timedelta64[us]",
}


class QueryProtocol(Protocol):
    def query(self, query: str) -> DataFrame: ...
