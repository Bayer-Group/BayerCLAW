from dataclasses import dataclass, field
import json


@dataclass
class S3File:
    bucket: str
    key: str

    def __repr__(self):
        return f"s3://{self.bucket}/{self.key}"


@dataclass
class Repo:
    bucket: str
    prefix: str

    @classmethod
    def from_uri(cls, uri: str):
        bucket, key = uri.split("/", 3)[2:]
        return cls(bucket, key)

    def qualify(self, uri: str) -> S3File:
        if uri.startswith("s3://"):
            ret = S3File(*uri.split("/", 3)[2:])
        else:
            wtf = f"{self.prefix}/{uri}"
            ret = S3File(bucket=self.bucket, key=f"{self.prefix}/{uri}")
        return ret

    def sub_repo(self, name):
        ret = Repo(self.bucket, f"{self.prefix}/{name}")
        return ret

    @property
    def job_data_file(self) -> S3File:
        return self.qualify("_JOB_DATA_")

    def __repr__(self):
        return f"s3://{self.bucket}/{self.prefix}"


# https://stackoverflow.com/questions/51286748/make-the-python-json-encoder-support-pythons-new-dataclasses
class RepoEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, (Repo, S3File)):
            return str(o)
        else:
            return super().default(o)


class OtherEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Repo):
            return {"bucket": o.bucket, "prefix": o.prefix, "uri": str(o)}
        else:
            return super().default(o)
