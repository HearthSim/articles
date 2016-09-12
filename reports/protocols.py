"""
Reusable MRJob protocols to give MRJob scripts access to HSReplay.net objects.
"""

import boto3
from gzip import decompress
from io import StringIO
from hsreplay.document import HSReplayDocument


S3 = boto3.client("s3")


class HSReplayS3Protocol:
	def read(self, line):
		# Each line should be to a hsreplay.xml file
		bucket, sep, key = line.decode("utf8").partition(":")
		obj = S3.get_object(Bucket=bucket, Key=key)
		xml_str = decompress(obj["Body"].read()).decode("utf8")

		shortid = key[25:-13]
		replay = HSReplayDocument.from_xml_file(StringIO(xml_str))

		return shortid, replay


class PowerlogS3Protocol:
	DEBUG = True

	def read_s3(self, bucket, key):
		obj = S3.get_object(Bucket=bucket, Key=key)
		log_str = decompress(obj["Body"].read()).decode("utf8")
		out = StringIO()
		out.write(log_str)
		out.seek(0)

		return out

	def read(self, line):
		# Each line should be to a power.log
		bucket, sep, key = line.decode("utf8").partition(":")

		if bucket == "local":
			out = open(key)
		else:
			try:
				out = self.read_s3(bucket, key)
			except Exception:
				if self.DEBUG:
					raise
				else:
					return None, ""

		return line, out
