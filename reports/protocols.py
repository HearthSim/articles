"""
Reusable MRJob protocols to give MRJob scripts access to HSReplay.net objects.
"""

import boto3
from gzip import decompress
from io import StringIO
from hsreplay.document import HSReplayDocument


S3 = boto3.client("s3")


class BaseS3Protocol:
	DEBUG = True

	def read_s3(self, bucket, key):
		obj = S3.get_object(Bucket=bucket, Key=key)
		log_str = decompress(obj["Body"].read()).decode("utf8")
		out = StringIO()
		out.write(log_str)
		out.seek(0)

		return out

	def get_file_handle(self, line):
		bucket, sep, key = line.decode("utf-8").partition(":")
		if bucket == "local":
			# Local filesystem handle
			return open(key, "r")

		try:
			return self.read_s3(bucket, key)
		except Exception:
			if self.DEBUG:
				raise
			else:
				return line, None


class HSReplayS3Protocol(BaseS3Protocol):
	def read(self, line):
		fh = self.get_file_handle(line)
		if not fh:
			return line, None
		try:
			replay = HSReplayDocument.from_xml_file(fh)
		except Exception:
			if self.DEBUG:
				raise
			else:
				return line, None
		return line, replay


class PowerlogS3Protocol(BaseS3Protocol):
	def read(self, line):
		return line, self.get_file_handle(line)
