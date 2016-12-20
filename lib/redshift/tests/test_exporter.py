import os
from hsreplay.document import HSReplayDocument
from etl.exporters import RedshiftPublishingExporter
from hearthstone.enums import BnetGameType


BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..")
DATA_DIR = os.path.join(BASE_DIR, "build", "hsreplay-test-data")
REPLAY_XML = os.path.join(
	DATA_DIR,
	"hsreplaynet-tests",
	"replays",
	"hsreplay.xml"
)


def test_redshift_exporter():
	if os.path.exists(REPLAY_XML):
		replay = HSReplayDocument.from_xml_file(open(REPLAY_XML))
		packet_tree = replay.to_packet_tree()[0]
		exporter = RedshiftPublishingExporter(packet_tree).export()

		# This is the full set of metadata that the exporter cannot discover by itself.
		# Must be provided via an external mechanism.
		game_info = {
			"game_id": 438972,
			"game_type": BnetGameType.BGT_RANKED_STANDARD.value,
			"scenario_id": 23,
			"ladder_season": 53,
			"brawl_season": 201,
			"players": {
				"1": {
					"deck_id": 7,
					"deck_list": "{\"2648\":2,\"38538\":1,\"1063\":1,\"475\":1,\"643\":1,\"739\":1,\"679\":1,\"2948\":1,\"1016\":1,\"40371\":2,\"510\":1}",
					"rank":0,
					"legend_rank": 3251,
					"full_deck_known": True
				},
				"2": {
					"deck_id": 9,
					"rank": 7,
					"legend_rank": None,
					"full_deck_known": False
				},
			}
		}

		exporter.set_game_info(game_info)
		block_records = exporter.get_block_records()
		assert len(block_records) == 95

		block_info_records = exporter.get_block_info_records()
		assert len(block_info_records) == 49

		entity_state_records = exporter.get_entity_state_records()
		assert len(entity_state_records) == 525

		player_records = exporter.get_player_records()
		assert len(player_records) == 2

		game_records = exporter.get_game_records()
		assert len(game_records) == 1

		choices_records = exporter.get_choice_records()
		assert len(choices_records) == 26

		option_records = exporter.get_option_records()
		assert len(option_records) == 315
	else:
		assert False, "Required replay.xml file does not exist. Have you run $ ./scripts/update_log_data.sh recently?"
