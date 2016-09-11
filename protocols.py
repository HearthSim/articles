"""Reusable MRJob protocols to give MRJob scripts access to HSReplay.net objects."""
from io import StringIO
from gzip import decompress
from hsreplay.document import HSReplayDocument
import boto3


S3 = boto3.client("s3")


class HSReplayS3Protocol(object):

	def read(self, line):
		# Each line should be to a hsreplay.xml file
		try:
			bucket, sep, key = line.decode("utf8").partition(":")
			obj = S3.get_object(Bucket=bucket, Key=key)
			xml_str = decompress(obj["Body"].read()).decode("utf8")

			shortid = key[25:-13]
			replay = HSReplayDocument.from_xml_file(StringIO(xml_str))
		except Exception as e:
			# TODO: Find out why from_xml_file fails on some replays
			return None, None
		else:
			return shortid, replay


class PowerlogS3Protocol(object):

	def read(self, line):
		try:
			# Each line should be to a power.log
			bucket, sep, key = line.decode("utf8").partition(":")
			if bucket == "local":
				out = open(key)
			else:
				obj = S3.get_object(Bucket=bucket, Key=key)
				log_str = decompress(obj["Body"].read()).decode("utf8")

				out = StringIO()
				out.write(log_str)
				out.seek(0)

		except Exception as e:
			return None, None
		else:
			return line, out
