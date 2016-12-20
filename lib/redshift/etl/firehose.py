"""
AWS Kinesis Firehose abstracts away the mechanics around efficiently batching and loading rows into Redshift.

For further details, see:
http://docs.aws.amazon.com/firehose/latest/dev/what-is-this-service.html
"""
import time
import boto3


FIREHOSE = boto3.client("firehose")


def publish_from_iterable_at_fixed_speed(
	iterable,
	publisher_func,
	max_records_per_second,
	publish_batch_size=1
):
	if max_records_per_second == 0:
		raise ValueError("times_per_second must be greater than 0!")

	finished = False
	while not finished:
		try:
			start_time = time.time()
			records_this_second = 0
			while not finished and records_this_second < max_records_per_second:
				batch = next_record_batch_of_size(iterable, publish_batch_size)
				if batch:
					records_this_second += len(batch)
					publisher_func(batch)
				else:
					finished = True

			if not finished:
				elapsed_time = time.time() - start_time
				sleep_duration = 1 - elapsed_time
				if sleep_duration > 0:
					time.sleep(sleep_duration)
		except StopIteration:
			finished = True


def next_record_batch_of_size(iterable, max_batch_size):
	result = []
	count = 0
	record = next(iterable, None)
	while record and count < max_batch_size:
		result.append(record)
		count += 1
		record = next(iterable, None)
	return result


class FirehoseOutput(object):
	def __init__(self, stream_name, max_records_per_sec=5000, batch_size=500):
		self._stream_name = stream_name
		self._max_records_per_sec = max_records_per_sec
		self._batch_size = batch_size

	def _publish_function(self, batch):
		return FIREHOSE.put_record_batch(
			DeliveryStreamName=self._stream_name,
			Records=batch
		)

	def write(self, records):
		publish_from_iterable_at_fixed_speed(
			iter(records),
			self._publish_function,
			self._max_records_per_sec,
			self._batch_size
		)

