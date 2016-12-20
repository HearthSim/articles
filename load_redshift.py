"""
NOTE: Make sure you have packaged up the latest libraries and ./lib is on the python path, e.g.

$ ./package_libraries.sh
$ PYTHONPATH=$PYTHONPATH:lib python load_redshift.py -r emr --conf-path mrjob.conf --cluster-id <CLUSTER_ID> <INPUTS_FILE> --no-output

See the ./lib/redshift/tests/* for examples of the expected metadata.
"""
from mapred.protocols import BaseJob
from redshift.etl.exporters import RedshiftPublishingExporter
from redshift.etl.records import (
	GameRecord, PlayerRecord, BlockRecord, BlockInfoRecord, ChoicesRecord,
	OptionsRecord, EntityStateRecord
)


def handle_replay(self, replay, metadata):
	try:
		packet_tree = replay.to_packet_tree()[0]
		exporter = RedshiftPublishingExporter(packet_tree).export()
		exporter.set_game_info(metadata)

		game_record_output = GameRecord.get_firehose_output()
		game_record_output.write(exporter.get_game_records())

		player_record_output = PlayerRecord.get_firehose_output()
		player_record_output.write(exporter.get_player_records())

		block_record_output = BlockRecord.get_firehose_output()
		block_record_output.write(exporter.get_block_records())

		block_info_record_output = BlockInfoRecord.get_firehose_output()
		block_info_record_output.write(exporter.get_block_info_records())

		choices_record_output = ChoicesRecord.get_firehose_output()
		choices_record_output.write(exporter.get_choice_records())

		entity_state_record_output = EntityStateRecord.get_firehose_output()
		entity_state_record_output.write(exporter.get_entity_state_records())

		options_record_output = OptionsRecord.get_firehose_output()
		options_record_output.write(exporter.get_option_records())
	except Exception as e:
		self.increment_counter("exceptions", e.__class__.__name__)
		self.increment_counter("error_global_game_ids", metadata["game_id"])
	return str(metadata["game_id"])


class Job(BaseJob):
	handler_function = handle_replay


if __name__ == "__main__":
	Job.run()
