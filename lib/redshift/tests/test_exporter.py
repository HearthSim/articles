import os
import time
import pytest
from hsreplay.document import HSReplayDocument
from etl.exporters import RedshiftPublishingExporter
from hearthstone.enums import BnetGameType, Step, GameTag
import sys
from numbers import Number
from collections import Set, Mapping, deque

zero_depth_bases = (str, bytes, Number, range, bytearray)
iteritems = 'items'

def getsize(obj_0):
	"""Recursively iterate to sum size of object & members."""
	def inner(obj, _seen_ids = set()):
		obj_id = id(obj)
		if obj_id in _seen_ids:
			return 0
		_seen_ids.add(obj_id)
		size = sys.getsizeof(obj)
		if isinstance(obj, zero_depth_bases):
			pass # bypass remaining control flow and return
		elif isinstance(obj, (tuple, list, Set, deque)):
			size += sum(inner(i,_seen_ids) for i in obj)
		elif isinstance(obj, Mapping) or hasattr(obj, iteritems):
			size += sum(inner(k,_seen_ids) + inner(v,_seen_ids) for k, v in getattr(obj, iteritems)())
		# Check for custom object instances - may subclass above too
		if hasattr(obj, '__dict__'):
			size += inner(vars(obj),_seen_ids)

		# if hasattr(obj, '__slots__'): # can have __slots__ with __dict__
		# 	size += sum(inner(getattr(obj, s),_seen_ids) for s in obj.__slots__ if hasattr(obj, s))
		return size
	return inner(obj_0)

BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..")
DATA_DIR = os.path.join(BASE_DIR, "build", "hsreplay-test-data")
REPLAY_XML = os.path.join(
	DATA_DIR,
	"hsreplaynet-tests",
	"replays",
	"annotated.hsreplay.xml"
)
ZERUS2_XML = os.path.join(
	DATA_DIR,
	"hsreplaynet-tests",
	"replays",
	"annotated.zerus2.hsreplay.xml"
)


def assert_in_entity_state_records(entity_state_records, entity_id, dbf_id = None, tags = {}):
	record = None
	for rec in entity_state_records:
		if rec._col_entity_id == entity_id:
			record = rec
	assert record is not None

	if dbf_id:
		assert record._col_dbf_id == dbf_id

	record_tags = record._tags_snapshot
	for tag, value in tags.items():
		assert record_tags[tag.value] == value, tag


def test_exporter_on_zerus2():
	if os.path.exists(ZERUS2_XML):
		replay = HSReplayDocument.from_xml_file(open(ZERUS2_XML))
		packet_tree = replay.to_packet_tree()[0]
		start_time = time.time()
		exporter = RedshiftPublishingExporter(packet_tree).export()
		end_time = time.time()
		duration = end_time - start_time
		exporter_mem_size = getsize(exporter)
		print("Memory: %s Duration: %s" % (exporter_mem_size, duration))


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

		atiesh_pred = lambda r: r._col_entity_id == 93
		atiesh_records = exporter.get_raw_entity_state_records_for_predicate(atiesh_pred)
		turn21_entity_state_records = exporter.get_raw_entity_state_records(21, Step.MAIN_READY)
		# Atiesh (Jaina's weapon from Mediv), Entity: 93, Controller: 2, ATK: 1, DURABILITY: 2, card_id: KAR_097t, dbf: 40360
		assert_in_entity_state_records(
			turn21_entity_state_records,
			93,
			40360,
			{
				GameTag.ATK: 1,
				GameTag.DURABILITY: 3,
				GameTag.DAMAGE: 1, # The current durability of a weapon is DURABILITY - DAMAGE
				GameTag.CONTROLLER: 1 # This is the playerID, not the entityID
			}
		)

		block_records = exporter.get_block_records()
		assert len(block_records) == 157

		block_info_records = exporter.get_block_info_records()
		assert len(block_info_records) == 64

		# pretty_tags = lambda tags: [(GameTag(k).name, v) for k,v in tags.items() if k in list(GameTag.__members__.values())]
		# def apply_pretty_tags(e):
		# 	e.pretty_tags = pretty_tags(e.tags)
		# zone_play[1].pretty_tags = pretty_tags(zone_play[1].tags)
		# zone_play = list(map(apply_pretty_tags, list(self.game.in_zone(Zone.PLAY))))

		# TODO: Add a method exporter.get_raw_entity_state_tags(turn=21, step=MAIN_READY)
		# Assert that the result set is exaclty this.
		# Use Replay Zerus2 to for an example of a secret in play.
		# When Turn = 21, After STEP = MAIN_READY and Before STEP = MAIN_START_TRIGGERS
		# Ln: 4493 Is When the Turn Increments
		# https://hsreplay.net/replay/r3sycz4JfMV8B5tR3kVxxS#turn=11a
		# The following entities should get snapshotted.
		# PLAY:
		#	Game Entity (id = 1)
		#	Player entity ekahS, entityid = 2
		#	Player entity Ramter, entityid = 3
		# 	(Hero) Rexxar, Entity: 66, Health: 18, FROZEN!
		#	Steady Shot, Entity: 67
		#	(assert Rexar has nothing on his side of the board)

		#	(Hero) Jaina, Entity: 64, Health: 7
		#	Fireblast, Entity: 65,
		# 	Atiesh (her weapon from Mediv), Entity: 93, Controller: 2, ATK: 1, DURABILITY: 2, card_id: KARA_13_26
		# Her Board:
		#	Position: 1, Entity: 27, Mediv, The Guardian, 7/7
		#	Position: 2, Entity: 19, Name: Mana Wyrm, ATK: 9, CURRENT_HEALTH: 2, Controller: 2
		#	Entity: 88, Name: Bananas (Enchantment), Controller: 2
		#	Entity: 89, Name: Bananas (Enchantment), Controller: 2
		#	Position: 3, Entity: 94 Name: King's Elekk, 3/2

		# SECRET:
		# 	Entity: 90, Name: Counterspell, Zone_position: 0 Controller: 2

		# HAND - Jaina:
		#	Position: 1, Entity: 26, Name: Flamestrike
		#	Position: 2, Entity: 30, Name: Arcane Blast
		#	Position: 3, Entity: 10, Name: Pyroblast
		#	Position: 4, Entity: 29, Name: Arcane Intellect
		#	Position: 5, Entity: 15, Name: Azure Drake, Cost: 7
		#	Position: 6, Entity: 31, Name: Fireball
		#	Position: 7, Entity: 91, Name: Arcane Intellect
		#	Position: 8, Entity: 7, Name: Flamestrike

		# HAND - Rexar
		# We must assert that we know entity 52 is EX1_538 (Unleash The Hounds), since we resolve entities at end of game
		# But Revealed = FALSE for this entity as of the start of Turn 22
		# Position: 1, Entity: 52, Revealed: False, Name: Unleash the Hounds, Controller: 3
		# Position: 2, Entity: 71 (This entity is never revealed so assert we don't know it's card ID)

		entity_state_records = exporter.get_entity_state_records()
		assert len(entity_state_records) == 1073

		player_records = exporter.get_player_records()
		assert len(player_records) == 2

		game_records = exporter.get_game_records()
		assert len(game_records) == 1

		choices_records = exporter.get_choice_records()
		assert len(choices_records) == 8

		option_records = exporter.get_option_records()
		assert len(option_records) == 455


	else:
		assert False, "Required zerus2.replay.xml file does not exist. Have you run $ ./scripts/update_log_data.sh recently?"


def test_exporter_on_hsreplay():
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
		assert len(entity_state_records) == 869

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
