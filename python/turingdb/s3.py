import os
from typing import Optional

from .exceptions import TuringDBException
from .path import PathType
from .protocol import QueryProtocol


class S3Client:
    def __init__(
        self,
        bucket_name: str,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        region: Optional[str] = None,
        use_scratch: bool = True,
    ):
        import boto3

        self._use_scratch = use_scratch
        self._scratch_folder = "__turing__scratch__"

        # Credentials are optional, they override the installed credentials
        self._s3_session = boto3.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
        )

        self._s3 = self._s3_session.client("s3")
        self._s3_resource = self._s3_session.resource("s3")
        self._s3_bucket = self._s3_resource.Bucket(bucket_name)  # type: ignore

        credentials = self._s3_session.get_credentials()
        assert credentials is not None
        region = self._s3_session.region_name
        secret_key = credentials.secret_key
        access_key = credentials.access_key

        self._access_key = access_key
        self._secret_key = secret_key
        self._region = region

        if region is None:
            region = ""

        self._bucket_name = bucket_name

    def connect(self, query_protocol: QueryProtocol):
        self._query_protocol = query_protocol
        self._query_protocol.query(
            f'S3 CONNECT "{self._access_key}" "{self._secret_key}" "{self._region}"'
        )

    def transfer(self, src: str, dst: str):
        import uuid
        from pathlib import Path

        if self._query_protocol is None:
            raise TuringDBException("TuringDB client is not connected")

        src_type = PathType.get_type(src)
        dst_type = PathType.get_type(dst)

        match src_type, dst_type:
            case PathType.LOCAL, PathType.LOCAL:
                raise NotImplementedError("Local to local transfer is not implemented")
            case PathType.S3, PathType.S3:
                raise NotImplementedError("S3 to S3 transfer is not implemented")
            case PathType.TURINGDB, PathType.TURINGDB:
                raise NotImplementedError(
                    "TuringDB to TuringDB transfer is not implemented"
                )

            # Local <-> S3
            case PathType.LOCAL, PathType.S3:
                src = str(Path(src).expanduser().resolve())
                dst = dst.replace("s3://", "")

                if os.path.isdir(src):
                    raise NotImplementedError("Directory upload is not implemented")

                self._s3.upload_file(src, self._bucket_name, dst)
            case PathType.S3, PathType.LOCAL:
                src = src.replace("s3://", "")
                dst = str(Path(dst).expanduser().resolve())

                self._s3.download_file(self._bucket_name, src, dst)

            # TuringDB <-> S3
            case PathType.TURINGDB, PathType.S3:
                src = src.replace("turingdb://", "")
                dst = dst.replace("s3://", f"s3://{self._bucket_name}/")

                if os.path.isdir(src):
                    raise NotImplementedError("Directory upload is not implemented")

                query = f'S3 PUSH "{src}" "{dst}"'
                self._query_protocol.query(query)
            case PathType.S3, PathType.TURINGDB:
                src = src.replace("s3://", f"s3://{self._bucket_name}/")
                dst = dst.replace("turingdb://", "")
                query = f'S3 PULL "{src}" "{dst}"'
                self._query_protocol.query(query)

            # TuringDB <-> Local
            case PathType.LOCAL, PathType.TURINGDB:
                if not self._use_scratch:
                    raise TuringDBException(
                        "Scratch is not enabled, please transfer to s3 first, then to TuringDB"
                    )

                s3_path = f"s3://{self._scratch_folder}/{uuid.uuid4().hex}"

                try:
                    self.transfer(src, s3_path)
                    self.transfer(s3_path, dst)
                finally:
                    self._s3_bucket.objects.filter(
                        Prefix=f"{self._scratch_folder}/"
                    ).delete()

            case PathType.TURINGDB, PathType.LOCAL:
                if not self._use_scratch:
                    raise TuringDBException(
                        "Scratch is not enabled, please transfer to s3 first, then to local"
                    )

                s3_path = f"s3://{self._scratch_folder}/{uuid.uuid4().hex}"

                try:
                    self.transfer(src, s3_path)
                    self.transfer(s3_path, dst)
                finally:
                    self._s3_bucket.objects.filter(
                        Prefix=f"{self._scratch_folder}/"
                    ).delete()
