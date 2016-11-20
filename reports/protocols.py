"""
Reusable MRJob protocols to give MRJob scripts access to HSReplay.net objects.
"""

import boto3
import json
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

	def read_line_protocol(self, line):
		bucket, sep, key = line.decode("utf-8").partition(":")
		metadata = {}
		if ":" in key:
			# Extended format
			key, sep, metadata = key.partition(":")
			metadata = json.loads(metadata)
		return bucket, key, metadata

	def get_file_handle(self, bucket, key):
		if bucket == "local":
			# Local filesystem handle
			return open(key, "r")

		try:
			return self.read_s3(bucket, key)
		except Exception:
			if self.DEBUG:
				raise


class HSReplayS3Protocol(BaseS3Protocol):
	def read(self, line):
		bucket, key, metadata = self.read_line_protocol(line)
		fh = self.get_file_handle(bucket, key)
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
