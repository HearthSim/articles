import os
import time
import pytest
from hsreplay.document import HSReplayDocument
from etl.exporters import RedshiftPublishingExporter
from hearthstone.enums import (
	BnetGameType, Step, GameTag, Zone, CardType, BlockType, MetaDataType
)
import sys
from numbers import Number
from collections import Set, Mapping, deque

BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..")
DATA_DIR = os.path.join(BASE_DIR, "build", "hsreplay-test-data")
FRIENDLY_ZERUS = os.path.join(
	DATA_DIR,
	"hsreplaynet-tests",
	"replays",
	"annotated.friendly_zerus.hsreplay.xml"
)
ZERUS2_XML = os.path.join(
	DATA_DIR,
	"hsreplaynet-tests",
	"replays",
	"annotated.zerus2.hsreplay.xml"
)


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


def assert_options_contains(options, entity, suboption_entity=None, target_entity=None, sent=False):
	for option in options:
		if option._col_option_entity_id == entity:
			if option._col_suboption_entity_id == suboption_entity:
				if option._col_target_entity_id == target_entity:
					if option._col_sent == sent:
						return True

	return False


def test_exporter_on_opposing_zerus():
	if os.path.exists(ZERUS2_XML):
		# https://hsreplay.net/replay/r3sycz4JfMV8B5tR3kVxxS
		replay = HSReplayDocument.from_xml_file(open(ZERUS2_XML))
		packet_tree = replay.to_packet_tree()[0]
		start_time = time.time()
		exporter = RedshiftPublishingExporter(packet_tree).export()
		end_time = time.time()
		duration = end_time - start_time
		exporter_mem_size_mb = ((1.0 * getsize(exporter)) / 1024.0) / 1024.0
		print("\nMemory (MB): %s Duration: %s" % (exporter_mem_size_mb, duration))

		# This is the full set of metadata that the exporter cannot discover by itself.
		# Must be provided via an external mechanism.
		game_info = {
			"game_id": 438972,
			"shortid": "r3sycz4JfMV8B5tR3kVxxS",
			"game_type": BnetGameType.BGT_RANKED_STANDARD.value,
			"scenario_id": 23,
			"ladder_season": 53,
			"brawl_season": 201,
			"players": {
				"1": {
					"deck_id": 7,
					"archetype_id": None,
					"deck_list": "{\"2648\":2,\"38538\":1,\"1063\":1,\"475\":1,\"643\":1,\"739\":1,\"679\":1,\"2948\":1,\"1016\":1,\"40371\":2,\"510\":1}",
					"rank":0,
					"legend_rank": 3251,
					"full_deck_known": True
				},
				"2": {
					"deck_id": 9,
					"archetype_id": None,
					"rank": 7,
					"legend_rank": None,
					"full_deck_known": False
				},
			}
		}
		exporter.set_game_info(game_info)

		# The next set of assertions is based on a detailed analysis of the snapshot taken at the start of Turn 21
		# From the ZERUS2 Test Replay File
		# https://hsreplay.net/replay/r3sycz4JfMV8B5tR3kVxxS#turn=11a
		# In The XML, Turn 21 starts on Line: 4493

		# We assert that there are exactly 24 records created and then we assert that each of them is
		# Exactly how we expect it to be.
		turn21_entity_state_pred = exporter.make_turn_step_predicate(21, Step.MAIN_READY)
		turn21_entity_state_records = exporter.get_entity_state_records(
			filter_predicate=turn21_entity_state_pred,
			as_data_records=False
		)
		assert len(turn21_entity_state_records) == 24
		assert all(map(lambda r: r._col_after_block_id is not None, turn21_entity_state_records))

		# HAND - Jaina:
		#	Position: 1, Entity: 26, Name: Flamestrike
		assert_in_entity_state_records(
			turn21_entity_state_records,
			26,
			1004,
			{
				GameTag.ZONE_POSITION: 1,
				GameTag.ZONE: Zone.HAND.value,
				GameTag.CONTROLLER: 1
			}
		)
		#	Position: 2, Entity: 30, Name: Arcane Blast
		assert_in_entity_state_records(
			turn21_entity_state_records,
			30,
			2572,
			{
				GameTag.ZONE_POSITION: 2,
				GameTag.ZONE: Zone.HAND.value,
				GameTag.CONTROLLER: 1
			}
		)
		#	Position: 3, Entity: 10, Name: Pyroblast
		assert_in_entity_state_records(
			turn21_entity_state_records,
			10,
			1087,
			{
				GameTag.ZONE_POSITION: 3,
				GameTag.ZONE: Zone.HAND.value,
				GameTag.CONTROLLER: 1
			}
		)
		#	Position: 4, Entity: 29, Name: Arcane Intellect
		assert_in_entity_state_records(
			turn21_entity_state_records,
			29,
			555,
			{
				GameTag.ZONE_POSITION: 4,
				GameTag.ZONE: Zone.HAND.value,
				GameTag.CONTROLLER: 1
			}
		)
		#	Position: 5, Entity: 15, Name: Azure Drake, Cost: 7
		assert_in_entity_state_records(
			turn21_entity_state_records,
			15,
			825,
			{
				GameTag.ZONE_POSITION: 5,
				GameTag.ZONE: Zone.HAND.value,
				GameTag.COST: 7,
				GameTag.CONTROLLER: 1
			}
		)
		#	Position: 6, Entity: 31, Name: Fireball
		assert_in_entity_state_records(
			turn21_entity_state_records,
			31,
			315,
			{
				GameTag.ZONE_POSITION: 6,
				GameTag.ZONE: Zone.HAND.value,
				GameTag.CONTROLLER: 1
			}
		)
		#	Position: 7, Entity: 91, Name: Arcane Intellect
		assert_in_entity_state_records(
			turn21_entity_state_records,
			91,
			555,
			{
				GameTag.ZONE_POSITION: 7,
				GameTag.ZONE: Zone.HAND.value,
				GameTag.CONTROLLER: 1
			}
		)
		#	Position: 8, Entity: 7, Name: Flamestrike
		assert_in_entity_state_records(
			turn21_entity_state_records,
			7,
			1004,
			{
				GameTag.ZONE_POSITION: 8,
				GameTag.ZONE: Zone.HAND.value,
				GameTag.CONTROLLER: 1
			}
		)

		# PLAY - Jaina
		#	Player entity ekahS, entityid = 2
		assert_in_entity_state_records(
			turn21_entity_state_records,
			2,
			tags = {
				GameTag.ZONE: Zone.PLAY.value,
				GameTag.CURRENT_PLAYER: 0,
				GameTag.CONTROLLER: 1,
				GameTag.WEAPON: 93 # 93 is the Entity ID for Atiesh
			}
		)
		#	(Hero) Jaina, Entity: 64, Health: 7
		assert_in_entity_state_records(
			turn21_entity_state_records,
			64,
			637,
			{
				GameTag.ZONE: Zone.PLAY.value,
				GameTag.CONTROLLER: 1,
				GameTag.HEALTH: 30,
				GameTag.DAMAGE: 23,
			}
		)
		#	Fireblast, Entity: 65,
		assert_in_entity_state_records(
			turn21_entity_state_records,
			65,
			807,
			{
				GameTag.ZONE: Zone.PLAY.value,
				GameTag.CONTROLLER: 1
			}
		)
		# Atiesh (Jaina's weapon from Mediv), Entity: 93, Controller: 1, ATK: 1, DURABILITY: 2, card_id: KAR_097t, dbf: 40360
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

		# Her Board:
		#	Position: 1, Entity: 27, Mediv, The Guardian, 7/7
		assert_in_entity_state_records(
			turn21_entity_state_records,
			27,
			39841,
			{
				GameTag.ATK: 7,
				GameTag.HEALTH: 7,
				GameTag.NUM_TURNS_IN_PLAY: 1,
				GameTag.ZONE_POSITION: 1,
				GameTag.CONTROLLER: 1
			}
		)
		#	Position: 2, Entity: 19, Name: Mana Wyrm, ATK: 9, CURRENT_HEALTH: 2, Controller: 1, num_turns_in_play: 5
		assert_in_entity_state_records(
			turn21_entity_state_records,
			19,
			405,
			{
				GameTag.ATK: 9,
				GameTag.HEALTH: 5, # Has been increased by 2 Bananas
				GameTag.DAMAGE: 3,
				GameTag.NUM_TURNS_IN_PLAY: 5,
				GameTag.ZONE_POSITION: 2,
				GameTag.CONTROLLER: 1
			}
		)
		#	Entity: 88, Name: Bananas (Enchantment), Controller: 1
		assert_in_entity_state_records(
			turn21_entity_state_records,
			88,
			1695,
			{
				GameTag.ATTACHED: 19, # This is an EntityID for Mana Wyrm
				GameTag.CARDTYPE: CardType.ENCHANTMENT.value,
				GameTag.NUM_TURNS_IN_PLAY: 5,
				GameTag.CONTROLLER: 1
			}
		)
		#	Entity: 89, Name: Bananas (Enchantment), Controller: 1
		assert_in_entity_state_records(
			turn21_entity_state_records,
			89,
			1695,
			{
				GameTag.ATTACHED: 19, # This is an EntityID for Mana Wyrm
				GameTag.CARDTYPE: CardType.ENCHANTMENT.value,
				GameTag.NUM_TURNS_IN_PLAY: 5,
				GameTag.CONTROLLER: 1
			}
		)
		#	Position: 3, Entity: 94 Name: King's Elekk, 3/2, CREATOR: 93
		assert_in_entity_state_records(
			turn21_entity_state_records,
			94,
			2635,
			{
				GameTag.ATK: 3,
				GameTag.HEALTH: 2,
				GameTag.CREATOR: 93, # Atiesh
				GameTag.NUM_TURNS_IN_PLAY: 1,
				GameTag.CONTROLLER: 1
			}
		)

		# SECRET:
		# 	Entity: 90, Name: Counterspell, Zone_position: 0 Controller: 2
		assert_in_entity_state_records(
			turn21_entity_state_records,
			90,
			113,
			{
				GameTag.CREATOR: 12, # Cabalist's Tome
				GameTag.SECRET: 1,
				GameTag.ZONE: Zone.SECRET.value,
				GameTag.CONTROLLER: 1
			}
		)

		# Now the second player!
		#	Player entity Ramter, entityid = 3
		assert_in_entity_state_records(
			turn21_entity_state_records,
			3,
			tags = {
				GameTag.ZONE: Zone.PLAY.value,
				GameTag.CURRENT_PLAYER: 1,
				GameTag.CONTROLLER: 2,
			}
		)
		# 	(Hero) Rexxar, Entity: 66, Health: 18, FROZEN!
		assert_in_entity_state_records(
			turn21_entity_state_records,
			66,
			31,
			{
				GameTag.ZONE: Zone.PLAY.value,
				GameTag.CONTROLLER: 2,
				GameTag.HEALTH: 30,
				GameTag.DAMAGE: 12,
				GameTag.FROZEN: 1,
			}
		)
		#	Steady Shot, Entity: 67
		assert_in_entity_state_records(
			turn21_entity_state_records,
			67,
			229,
			{
				GameTag.ZONE: Zone.PLAY.value,
				GameTag.CONTROLLER: 2
			}
		)

		# HAND - Rexar
		# We must assert that we know entity 52 is EX1_538 (Unleash The Hounds), since we resolve entities at end of game
		assert_in_entity_state_records(
			turn21_entity_state_records,
			52,
			1243,
			{
				GameTag.ZONE_POSITION: 1,
				GameTag.ZONE: Zone.HAND.value,
				GameTag.CONTROLLER: 2
			}
		)
		# Position: 2, Entity: 71 (This entity is never revealed so assert we don't know it's card ID)
		assert_in_entity_state_records(
			turn21_entity_state_records,
			71,
			tags={
				GameTag.ZONE_POSITION: 2,
				GameTag.ZONE: Zone.HAND.value,
				GameTag.CONTROLLER: 2
			}
		)

		block_records = exporter.get_block_records(as_data_records=False)
		assert len(block_records) == 157

		block_info_records = exporter.get_block_info_records(as_data_records=False)
		assert len(block_info_records) == 64

		# We do deep assertions about block creation based on the block starting on Line: 1966 (Turn: 10)
		block120_record = exporter.get_block_record_for_seq(120)
		block121_record = exporter.get_block_record_for_seq(121)
		block122_record = exporter.get_block_record_for_seq(122)
		assert block120_record._col_block_type == BlockType.PLAY.value
		assert block120_record._col_entity_id == 20 # Entity 20 is Arcane Blast
		assert block120_record._col_target_entity_id == 47 # Entity 47 is Prince Malchezaar

		assert block121_record._col_block_type == BlockType.POWER.value
		assert block121_record.parent_block_id == 120
		assert block121_record._col_entity_id == 20
		assert block121_record._col_target_entity_id == 47
		block121_info_records = exporter.get_block_info_records_for_seq(121)
		assert len(block121_info_records) == 2

		assert any(map(lambda r: r._col_meta_data_type == MetaDataType.TARGET, block121_info_records))
		assert any(map(lambda r: r._col_meta_data_type == MetaDataType.DAMAGE, block121_info_records))
		damage_info_record = list(filter(lambda r: r._col_meta_data_type == MetaDataType.DAMAGE, block121_info_records))[0]
		# Thalnos & Azure Drake are in play so Arcane Blast should be doing 6 damage
		assert damage_info_record._col_data == 6

		assert block122_record._col_block_type == BlockType.DEATHS.value
		assert block122_record.parent_block_id == 120

		entity_state_records = exporter.get_entity_state_records(as_data_records=False)
		assert len(entity_state_records) == 1065

		player_records = exporter.get_player_records(as_data_records=False)
		assert len(player_records) == 2

		game_records = exporter.get_game_records(as_data_records=False)
		assert len(game_records) == 1

		choices_records = exporter.get_choice_records(as_data_records=False)
		assert len(choices_records) == 8

		option_records = exporter.get_option_records(as_data_records=False)
		assert len(option_records) == 455

		# Let's do deep assertions on the correctness of the options block starting on Line: 1937
		options30 = list(filter(lambda r: r._col_options_block_id == 30, option_records))
		assert len(options30) == 20
		assert assert_options_contains(options30, 32, target_entity=64)
		assert assert_options_contains(options30, 32, target_entity=66)
		assert assert_options_contains(options30, 32, target_entity=74)
		assert assert_options_contains(options30, 32, target_entity=60)
		assert assert_options_contains(options30, 32, target_entity=15)
		assert assert_options_contains(options30, 32, target_entity=47)
		assert assert_options_contains(options30, 32, target_entity=6)
		assert assert_options_contains(options30, 32, target_entity=23)
		assert assert_options_contains(options30, 20, target_entity=74)
		assert assert_options_contains(options30, 20, target_entity=60)
		assert assert_options_contains(options30, 20, target_entity=15)
		assert assert_options_contains(options30, 20, target_entity=47, sent=True)
		assert assert_options_contains(options30, 20, target_entity=6)
		assert assert_options_contains(options30, 20, target_entity=23)
		assert assert_options_contains(options30, 24)
		assert assert_options_contains(options30, 15, target_entity=66)
		assert assert_options_contains(options30, 15, target_entity=74)
		assert assert_options_contains(options30, 15, target_entity=60)
		assert assert_options_contains(options30, 15, target_entity=47)

		# Finally, We dive into asserting we snapshotted Zerus correctly
		# Zerus is added to the deck by Prince Malchezar, then drawn on Turn 9
		# And played on Turn 13 as a Priest Of The Fiest
		zerus_entity_states = exporter.get_entity_states_for_entity(70)
		# Let's confirm that when initially drawn we ID the card as Zerus until it gets played
		assert zerus_entity_states[1]._col_dbf_id == 38475
		assert zerus_entity_states[1]._col_entity_in_initial_entities == False

		# Then the card gets played on block 181
		# We assert that its Zerus in the before block snapshot and Priest of the Feast after
		assert zerus_entity_states[9]._col_dbf_id == 38475
		assert zerus_entity_states[14]._col_dbf_id == 39442
	else:
		assert False, "Required zerus2.replay.xml file does not exist. Have you run $ ./scripts/update_log_data.sh recently?"


def test_exporter_on_friendly_zerus():
	"""
	Zerus CardID: OG_123, dbfID: 38475, entityID: 45, Zerus' Controller: 2

	In this replay, the following zerus events happen:
	- Argent Squire, Senjin, Eviscerate are the initial mulligan draws for the friendly player
	- Zerus is drawn by the first as a replacement for Senjin Shieldmasta during the mulligan
	- Zerus immediately becomes a Mistress Of Pain On Turn 1
	- Darkscale Healer is the first card drawn at the start of Turn 1
	- Zerus becomes a Sunwalker on Turn 3
	- Zerus becomes a Spellslinger on Turn 5
	- Zerus becomes a Void Terror on Turn 7
	- Zerus becomes a Puddle Stomper on Turn 9
	- Zerus becomes a Shielded Minibot on Turn 11
	- Zerus becomes a Streetwise Investigator on Turn 13
	- Zerus becomes Anamalous On Turn 15
	- Zerus becomes a Frostwolf Warlord on Turn 17
	- Zerus becomse a Cobalt Guardian on Turn 19
	- Zerus becomes Animated Armor on Turn 21
	- Zerus gets played (as Animated Armor) on Turn 21
	"""
	if os.path.exists(FRIENDLY_ZERUS):
		# https://hsreplay.net/replay/6cTCJQJJfqJCLjMgZEoBBX
		replay = HSReplayDocument.from_xml_file(open(FRIENDLY_ZERUS))
		packet_tree = replay.to_packet_tree()[0]
		start_time = time.time()
		exporter = RedshiftPublishingExporter(packet_tree).export()
		end_time = time.time()
		duration = end_time - start_time
		exporter_mem_size_mb = ((1.0 * getsize(exporter)) / 1024.0) / 1024.0
		print("\nMemory (MB): %s Duration: %s" % (exporter_mem_size_mb, duration))

		# This is the full set of metadata that the exporter cannot discover by itself.
		# Must be provided via an external mechanism.
		game_info = {
			"game_id": 438972,
			"shortid": "6cTCJQJJfqJCLjMgZEoBBX",
			"game_type": BnetGameType.BGT_RANKED_STANDARD.value,
			"scenario_id": 23,
			"ladder_season": 53,
			"brawl_season": 201,
			"players": {
				"1": {
					"deck_id": 7,
					"archetype_id": None,
					"deck_list": "{\"2648\":2,\"38538\":1,\"1063\":1,\"475\":1,\"643\":1,\"739\":1,\"679\":1,\"2948\":1,\"1016\":1,\"40371\":2,\"510\":1}",
					"rank":0,
					"legend_rank": 3251,
					"full_deck_known": True
				},
				"2": {
					"deck_id": 9,
					"archetype_id": None,
					"rank": 7,
					"legend_rank": None,
					"full_deck_known": False
				},
			}
		}

		exporter.set_game_info(game_info)
		block_records = exporter.get_block_records(as_data_records=False)
		assert len(block_records) == 197

		block_info_records = exporter.get_block_info_records(as_data_records=False)
		assert len(block_info_records) == 89

		entity_state_records = exporter.get_entity_state_records(as_data_records=False)
		assert len(entity_state_records) == 1355

		player_records = exporter.get_player_records(as_data_records=False)
		assert len(player_records) == 2

		game_records = exporter.get_game_records(as_data_records=False)
		assert len(game_records) == 1

		choices_records = exporter.get_choice_records(as_data_records=False)
		assert len(choices_records) == 11

		option_records = exporter.get_option_records(as_data_records=False)
		assert len(option_records) == 357

		# We do a deep dive into asserting we captured Zerus correctly
		zerus_entity_states = exporter.get_entity_states_for_entity(45)
		# Record 0 is Turn 1, MAIN_READY Zerus should still report the Zerus DBF ID as he has not shifted yet.
		assert zerus_entity_states[0]._col_dbf_id == 38475
		# Record 1 is Zerus's shifting block, but he should still report as Zerus in the before block snapshots
		assert zerus_entity_states[1]._col_dbf_id == 38475
		# In the after block snapshots he should report as Mistress of Pain
		assert zerus_entity_states[2]._col_dbf_id == 2172
		assert zerus_entity_states[2]._tags_snapshot[GameTag.TRANSFORMED_FROM_CARD] == 38475

		# In a <TagChange> after the initial trigger block the "Shifting" tag is also set on the entity
		# Therefore we don't assert that it has been set until a later entity_state

		# Records 5 and 6 are before and after the shifting block again
		assert zerus_entity_states[5]._col_dbf_id == 2172
		assert zerus_entity_states[5]._tags_snapshot[GameTag.SHIFTING] == 1
		assert zerus_entity_states[6]._col_dbf_id == 759

		# Shifter Zerus is finally played on block 350, Line: 6255
		# Record 43 is the before block
		assert zerus_entity_states[43]._col_dbf_id == 36111
		# Record 48 is the after block, by this time the SHIFTING tag and TRANSFORMED_FROM_CARD tags should be reset
		assert zerus_entity_states[48]._tags_snapshot[GameTag.TRANSFORMED_FROM_CARD] == 0
		assert zerus_entity_states[48]._tags_snapshot[GameTag.SHIFTING] == 0
	else:
		assert False, "Required replay.xml file does not exist. Have you run $ ./scripts/update_log_data.sh recently?"
