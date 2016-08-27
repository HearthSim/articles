"""
A reference example of an MRJob script for large scale ad-hoc replay analysis.

This job demonstrates the basic Map-Reduce pattern for horizontally scalable analysis over
large data sets (e.g. millions of replays). It is typically invoked in two ways:

$ python frequencies.py inputs.txt

Will run the job locally within a single process for easy prototyping and debugging. In this
mode inputs.txt should just be a couple of rows for the purpose of doing quick iterations.

$ run_emr_job.sh frequencies.py

Will spin up an Elastic Map Reduce (EMR) cluster from 1 - N machines. Where N can be set
by editing ./scripts/jobs/mrjob.conf and currently defaults to N = 4. The inputs.txt data
set will be evenly divided across all N machines and processed in parallel.

This bootstrapping process can add 5 - 10 minutes of overhead, therefore running on EMR
should not be done unless the size of the data set is large enough that the speedup
gained by 20x parallel processing (e.g. if N = 20) more than makes up for the additional
10 minutes of bootstrapping overhead. If we start running enough jobs in the future,
then this cluster can be made persistent which would then remove 90% of the job setup
overhead.

Jobs are running using EC2 spot instances, which are unused EC2 capacity that can be bid
on for transient use at a fraction of standard EC2 prices. The currently configured instance
type of m1.medium can cost up to $0.05 per machine per hour. However in practice they
will more frequently cost about $0.031 per machine hour. Therefore, as an example:

If N = 20 and the job takes less than 1 hour to complete it will cost: 20 * .031 = $0.62
If N = 100 and the job takes less than 1 hour to complete it will cost: 100 * .031 = $3.10

The inputs.txt file expected by jobs using the HSReplayS3Protocol must contain one line per
replay to be processed in the following format:

<BUCKET_NAME>:<REPLAY_XML_KEY>\n

The ./scripts/jobs/generate_mrjob_inputs.py file can be used to generate an inputs.txt file
that contains lines for the most recent M hour(s) of replays uploaded to the platform.

If the HSReplayS3Protocol is used as the INPUT_PROTOCOL then each line will be marshaled
into an instance of hsreplay.document.HSReplayDocument and the mappers will be sent
the SHORTID and the HSReplayDocument instance as arguments.
"""

from heapq import heappushpop, heappush
from mrjob.job import MRJob
from hsreplay.elements import PlayerNode
from .protocols import HSReplayS3Protocol


class CountPlayerEncounterFrequency(MRJob):
	"""
	A job to calculate the name of the player encountered most frequently across the
	input set of replays.

	This job only requires a single Map-Reduce pass, however more sophisticated types of
	analysis can easily chain several Map-Reduce steps together with the outputs of one
	step's reducers becoming the inputs for the next step's mappers.

	Many different types of ad-hoc analytics questions, statistical aggregations,
	and machine learning algorithms can be easily expressed in terms of Map-Reduce jobs.
	"""

	INPUT_PROTOCOL = HSReplayS3Protocol
	MAX_RESULTS = 10

	def mapper(self, shortid, replay):
		if shortid and replay:
			game_node = replay.games[0]
			players = filter(lambda n: isinstance(n, PlayerNode), game_node.nodes)
			for player in players:
				unique_player = "%s:%s" % (player.accountLo, player.name)
				occurance = 1
				# Each mapper will emit a record for each unique player observed
				yield unique_player, occurance
		else:
			# HSReplayDocument.from_xml_file() is failing on many replays
			# We can use counters to keep statistics on the performance of our jobs
			self.increment_counter('CountPlayerEncounterFrequency', 'null_inputs', 1)

	def combiner(self, unique_player, occurances):
		# Combiners are an optimization that will consolidate all the inputs from a mapper
		# into a more efficient format before they get sent across the wire to the reducer
		yield unique_player, sum(occurances)

	def reducer_init(self):
		self.heap = []

	def reducer(self, unique_player, occurances):
		# The Map-Reduce runtime guarantees that a single reducer will see all the values
		# emitted from the mappers for each unique key (in our case unique_player).
		# This allows the reducer to produce accurate counts for aggregate statistics
		account_lo, sep, player_name = unique_player.partition(":")
		total_occurances = sum(occurances)
		record = (total_occurances, player_name)

		# We are using a bounded heap to only keep the Top MAX_RESULTS in memory.
		# Since reducers see data from multiple mappers it's important to design them
		# So that they don't accidentally run out of memory.
		if len(self.heap) < self.MAX_RESULTS:
			heappush(self.heap, record)
		else:
			heappushpop(self.heap, record)

	def reducer_final(self):
		# This job will stream the output from the final reduce() step to the console.
		# If we set the right flags when executing the job it can write out to S3 instead.
		# If we configured an OUTPUT_PROTOCOL we could write to a DB or any other location.
		for total_occurances, player_name in reversed(self.heap):
			yield player_name, total_occurances

if __name__ == '__main__':
	CountPlayerEncounterFrequency.run()
