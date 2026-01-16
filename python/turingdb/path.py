from enum import Enum


class PathType(Enum):
    LOCAL = "local"
    S3 = "s3"
    TURINGDB = "turingdb"

    @staticmethod
    def get_type(path: str):
        if path.startswith("s3://"):
            return PathType.S3
        elif path.startswith("turingdb://"):
            return PathType.TURINGDB
        else:
            return PathType.LOCAL
