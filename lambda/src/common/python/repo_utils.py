from dataclasses import dataclass
import json


SYSTEM_FILE_TAG = "bclaw.system=true"

# @dataclass
# class S3File:
#     bucket: str
#     key: str
#
#     def __repr__(self):
#         return f"s3://{self.bucket}/{self.key}"
#
#     # def __eq__(self, other):
#     #     return self.bucket == other.bucket and self.key == other.key

class S3File(str):
    def __new__(cls, bucket: str, key: str):
        return str.__new__(cls, f"s3://{bucket}/{key}")

    def __init__(self, bucket: str, key: str):
        self.bucket = bucket
        self.key = key


# @dataclass
# class Repo:
#     bucket: str
#     prefix: str
#
#     @classmethod
#     def from_uri(cls, uri: str):
#         bucket, key = uri.split("/", 3)[2:]
#         return cls(bucket, key)
#
#     def qualify(self, uri: str) -> S3File:
#         if uri.startswith("s3://"):
#             ret = S3File(*uri.split("/", 3)[2:])
#         else:
#             wtf = f"{self.prefix}/{uri}"
#             ret = S3File(bucket=self.bucket, key=f"{self.prefix}/{uri}")
#         return ret
#
#     def sub_repo(self, name):
#         ret = Repo(self.bucket, f"{self.prefix}/{name}")
#         return ret
#
#     @property
#     def job_data_file(self) -> S3File:
#         return self.qualify("_JOB_DATA_")
#
#     def __repr__(self):
#         return f"s3://{self.bucket}/{self.prefix}"
#
#     # def __eq__(self, other):
#     #     return self.bucket == other.bucket and self.prefix == other.prefix

class Repo(dict):
    def __init__(self, *args, **kwargs):
        dict.__init__(self, *args, **kwargs)
        self.setdefault("uri", f"s3://{self.bucket}/{self.prefix}")

    @classmethod
    def from_uri(cls, uri: str):
        bucket, prefix = uri.split("/", 3)[2:]
        return cls(bucket=bucket, prefix=prefix)

    @property
    def bucket(self) -> str:
        return self["bucket"]

    @property
    def prefix(self) -> str:
        return self["prefix"]

    @property
    def uri(self) -> str:
        return self["uri"]

    def qualify(self, file_spec: str) -> S3File:
        if file_spec.startswith("s3://"):
            ret = S3File(*file_spec.split("/", 3)[2:])
        else:
            ret = S3File(self.bucket, f"{self.prefix}/{file_spec}")
        return ret

    def sub_repo(self, name):
        ret = Repo(bucket=self.bucket, prefix=f"{self.prefix}/{name}")
        return ret

    def __repr__(self) -> str:
        return self.uri


# https://stackoverflow.com/questions/51286748/make-the-python-json-encoder-support-pythons-new-dataclasses
# class RepoEncoder(json.JSONEncoder):
#     def default(self, o):
#         if isinstance(o, (Repo, S3File)):
#             return str(o)
#         else:
#             return super().default(o)


# class OtherEncoder(json.JSONEncoder):
#     def default(self, o):
#         if isinstance(o, Repo):
#             return {"bucket": o.bucket, "prefix": o.prefix, "uri": str(o)}
#         else:
#             return super().default(o)
