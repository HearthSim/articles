"""
An EntityTreeExporter implementation designed to generate the row Records that are expected by Redshift and can be
ETL'd in via Firehose. The setup.py script at the root of the `lib` module is intended to make this exporter packagable
so that it can be used within the HSReplay.net Lambda processing framework to load replay data into Redshift.
"""
from collections import defaultdict
from hearthstone.enums import Zone, GameTag, Step, BnetRegion
from hearthstone.hslog.export import EntityTreeExporter
from hearthstone.hslog import packets
from .records import (
	card_db, GameRecord, PlayerRecord, BlockRecord,
	BlockInfoRecord, ChoicesRecord, OptionsRecord, EntityStateRecord
)


class RedshiftPublishingExporter(EntityTreeExporter):
	def __init__(self, packet_tree):
		self._block_records = []
		self._player_records = []
		self._game_records = []
		self._block_info_records = []
		self._entity_state_records = []
		self._choice_records = []
		self._option_records = []
		self._db = card_db()
		self._block_stack = []
		self._choices_map = defaultdict(list)
		self._next_block_sequence_num = 1
		self._game_info_is_set = False
		self._first_ts_observed = None
		self._last_ts_observed = None
		self._region = None
		self._current_options_packet = None
		self._players_with_visible_options = set()
		super(RedshiftPublishingExporter, self).__init__(packet_tree)

	def handle_options(self, packet):
		self._players_with_visible_options.add(self.game.current_player)
		# This packet needs to be decomposed into multiple options records
		# The entire Option sub-tree is already pre-grouped as a packet
		# So we save it and then generate OptionRecords once we see the SentOption packet
		self._current_options_packet = packet
		super(RedshiftPublishingExporter, self).handle_options(packet)

	def handle_send_option(self, packet):
		super(RedshiftPublishingExporter, self).handle_send_option(packet)
		if self._current_options_packet:
			# If reconnecting or spectating midway through
			# It's possible we will have not seen the preceeding <Options> packet
			options_packet = self._current_options_packet
			sent_packet = packet
			self._generate_option_records([], options_packet, sent_packet)
			# Clear out the previous packet just for good house keeping once we're done
			self._current_options_packet = None

	def _generate_option_records(self, stack, options_packet, sent_packet):
		stack.append(options_packet)
		for option in options_packet.options:
			if not option.options:
				self._make_options_record(stack, option, sent_packet)
			else:
				self._generate_option_records(stack, option, sent_packet)
		stack.pop()

	def _make_options_record(self, stack, option, sent_packet):
		options_block_id = stack[0].id
		is_sent = sent_packet.option == option.id
		sent_position = sent_packet.position
		sent_suboption = sent_packet.suboption
		sent_target = sent_packet.target
		if len(stack) == 1:
			# This is a top-level option without sub-options or targets
			self._option_records.append(OptionsRecord(
				self.game,
				options_block_id,
				option.id,
				option.type,
				option.entity,
				None,
				None,
				None,
				None,
				is_sent,
				sent_position,
				sent_suboption,
				sent_target
			))
		elif len(stack) == 2:
			# This is a top level option with targets, or a sub-option without targets
			first_level_option = stack[1]
			if option.optype == 'subOption':
				# This is a sub-option without targets like Living Root Saplings
				self._option_records.append(OptionsRecord(
					self.game,
					options_block_id,
					first_level_option.id,
					first_level_option.type,
					first_level_option.entity,
					option.id,
					option.entity,
					None,
					None,
					is_sent,
					sent_position,
					sent_suboption,
					sent_target
				))
			else:
				# This is a top level block with targets
				self._option_records.append(OptionsRecord(
					self.game,
					options_block_id,
					first_level_option.id,
					first_level_option.type,
					first_level_option.entity,
					None,
					None,
					option.id,
					option.entity,
					is_sent,
					sent_position,
					sent_suboption,
					sent_target
				))
		elif len(stack) == 3:
			# This is a "choose one" sub-option with targets (e.g. Wrath, Living Roots)
			first_level_option = stack[1]
			sub_option = stack[2]
			self._option_records.append(OptionsRecord(
				self.game,
				options_block_id,
				first_level_option.id,
				first_level_option.type,
				first_level_option.entity,
				sub_option.id,
				sub_option.entity,
				option.id,
				option.entity,
				is_sent,
				sent_position,
				sent_suboption,
				sent_target
			))

	def handle_create_game(self, packet):
		super(RedshiftPublishingExporter, self).handle_create_game(packet)

		self._game_records.append(GameRecord(
			self.game,
			packet
		))

	def handle_player(self, packet):
		super(RedshiftPublishingExporter, self).handle_player(packet)

		if not self._region and hasattr(packet, "hi") and packet.hi:
			self._region = BnetRegion.from_account_hi(int(packet.hi)).value

		self._player_records.append(PlayerRecord(
			self.game,
			packet
		))

	def handle_metadata(self, packet):
		self.capture_block_info_record(packet)
		super(RedshiftPublishingExporter, self).handle_metadata(packet)

	def handle_block(self, block):
		# All blocks get assigned a sequence number to help generate primary keys
		self.assign_block_sequence_num(block)

		# Capture the timestamps so we can record game lengths in seconds
		self._capture_time_stamp(block)

		# We only record a sub-set of eligible blocks to reduce data volumes
		self.set_block_eligibility(block)

		if block.is_eligible_for_record:
			self.capture_before_block_entity_state_records(block)
			self.capture_block_record(block)

		# We maintain a stack of blocks so a sub-block can discover its parent
		self.push_block(block)
		super(RedshiftPublishingExporter, self).handle_block(block)
		self.pop_block(block)

		if block.is_eligible_for_record:
			self.capture_after_block_entity_state_records(block)

	def handle_show_entity(self, packet):
		tags = dict(packet.tags)
		if GameTag.ZONE in tags:
			self.record_zone_entrance(packet.entity)

		super(RedshiftPublishingExporter, self).handle_show_entity(packet)

	def handle_tag_change(self, packet):
		if packet.tag == GameTag.ZONE:
			self.record_zone_entrance(packet.entity)

		# We snapshot the entities in Zones: HAND, PLAY, and SECRET
		# at the start of each turn, before the player begins taking actions
		current_step_is_triggers = self.current_step() == Step.MAIN_START_TRIGGERS
		main_action_next = packet.tag == GameTag.STEP and packet.value == Step.MAIN_ACTION
		if current_step_is_triggers and main_action_next:
			self.snapshot_entities_in_zone(Zone.HAND)
			self.snapshot_entities_in_zone(Zone.PLAY)
			self.snapshot_entities_in_zone(Zone.SECRET)

		super(RedshiftPublishingExporter, self).handle_tag_change(packet)

		is_turn_one = self.current_turn() == 1
		is_main_ready = packet.tag == GameTag.STEP and packet.value == Step.MAIN_READY
		# On Turn 1, we snapshot Zone.HAND before the first card is drawn
		# for mulligan statistics
		if is_turn_one and is_main_ready:
			self.snapshot_entities_in_zone(Zone.HAND)

	def handle_full_entity(self, packet):
		super(RedshiftPublishingExporter, self).handle_full_entity(packet)
		tags = dict(packet.tags)
		if GameTag.ZONE in tags:
			self.record_zone_entrance(packet.entity)

	def handle_choices(self, packet):
		containing_block = self.peek_block()
		if containing_block and containing_block.is_eligible_for_record:
			for entity_id in packet.choices:
				record = ChoicesRecord(self.game, containing_block, packet, entity_id)
				self._choices_map[packet.id].append(record)
				self._choice_records.append(record)

	def handle_send_choices(self, packet):
		self.update_choices_with_selected(packet)

	def handle_chosen_entities(self, packet):
		self.update_choices_with_selected(packet)

	def update_choices_with_selected(self, packet):
		if packet.id in self._choices_map:
			for chosen_entity in packet.choices:
				for choice_record in self._choices_map[packet.id]:
					if choice_record._col_entity_id == chosen_entity:
						choice_record._col_chosen = True

	def snapshot_entities_in_zone(self, zone):
		for entity in self.game.in_zone(zone):
			self._entity_state_records.append(
				EntityStateRecord(
					self.game,
					entity
				)
			)

	def capture_block_record(self, block):
		# This should only be called when block eligibility has already been determined.
		parent_block = self.peek_block()
		self._block_records.append(
			BlockRecord(self.game, block, parent_block)
		)

	def capture_block_info_record(self, packet):
		containing_block = self.peek_block()
		if containing_block and containing_block.is_eligible_for_record:
			for info_entity_id in packet.info:
				self._block_info_records.append(
					BlockInfoRecord(self.game, containing_block, packet, info_entity_id)
				)

	def capture_before_block_entity_state_records(self, block):
		# Generate before_block_id entity_state records
		for entity in self._generate_block_manifest(block, is_before_block=True):
			self._entity_state_records.append(
				EntityStateRecord(
					self.game,
					entity,
					before_block_seq_num=block.block_sequence_num
				)
			)

	def capture_after_block_entity_state_records(self, block):
		# Generate after_block_id entity_state records
		# A new manifest is generated since new entities may have been created
		for entity in self._generate_block_manifest(block, is_before_block=False):
			self._entity_state_records.append(
				EntityStateRecord(
					self.game,
					entity,
					after_block_seq_num=block.block_sequence_num
				)
			)

	def set_game_info(self, game_info):
		# This information must be provided at the end of processing
		# Before the individual records will be complete and eligible for loading
		# Into Redshift
		self._game_info_is_set = True

		if "region" not in game_info and self._region:
			game_info["region"] = self._region

		if "game_length_seconds" not in game_info:
			if self._first_ts_observed and self._last_ts_observed:
				time_delta = self._last_ts_observed - self._first_ts_observed
				game_info["game_length_seconds"] = str(time_delta.seconds)

		if "match_start" not in game_info and self._first_ts_observed:
			game_info["match_start"] = self._first_ts_observed

		if "game_date" not in game_info and self._first_ts_observed:
			game_info["game_date"] = self._first_ts_observed.date()

		players_data = game_info.get("players", {})
		for player in self._players_with_visible_options:
			this_player_data = players_data.get(str(player.player_id), {})
			this_player_data["options_visible"] = True

		for rec in self._block_records:
			rec.set_game_info(game_info)

		for rec in self._block_info_records:
			rec.set_game_info(game_info)

		for rec in self._entity_state_records:
			rec.set_game_info(game_info)

		for rec in self._choice_records:
			rec.set_game_info(game_info)

		for rec in self._player_records:
			rec.set_game_info(game_info)

		for rec in self._game_records:
			rec.set_game_info(game_info)

		for rec in self._option_records:
			rec.set_game_info(game_info)

	# #### Utility Methods #### #

	def _capture_time_stamp(self, block):
		if block.ts:
			if not self._first_ts_observed:
				self._first_ts_observed = block.ts

			if not self._last_ts_observed or self._last_ts_observed < block.ts:
				self._last_ts_observed = block.ts


	def set_block_eligibility(self, block):
		# The BlockRecord class owns the logic for which blocks are eligible
		block.is_eligible_for_record = False
		for predicate in BlockRecord.BLOCK_ELIGABILITY_PREDICATES:
			if predicate(self.game, block):
				block.is_eligible_for_record = True

	def assign_block_sequence_num(self, block):
		block.block_sequence_num = self._next_block_sequence_num
		self._next_block_sequence_num += 1

	def push_block(self, block):
		self._block_stack.append(block)

	def pop_block(self, block):
		self._block_stack.pop()

	def peek_block(self):
		if len(self._block_stack):
			return self._block_stack[-1]
		else:
			return None

	def _filter_entity_from_manifest(self, entity, block, is_before_block=True):
		# This allows us to implement more selective filtering for entity_state records
		# The EntityStateRecord owns the rules for what should get filtered
		for filter_predicate in EntityStateRecord.ENTITY_STATE_FILTERING_PREDICATES:
			if filter_predicate(self.game, entity, block, is_before_block):
				return True

		return False

	def _generate_block_manifest(self, block, is_before_block=True):
		accumulator = set()
		self._accumulate_entities(accumulator, block, is_before_block)
		for entity_id in accumulator:
			entity = self.game.find_entity_by_id(entity_id)
			if entity:
				# entity can be None, because
				# In the before_block manifest new entities do not exist yet
				if not self._filter_entity_from_manifest(entity, block, is_before_block):
					yield entity

	def _accumulate_entities(self, accumulator, p, is_before_block=True):
		if isinstance(p, packets.Block):
			# Cache the manifest since we know we will need the manifest again for every
			# nested Block when its own handle_block() hook fires.
			cache_field = "_manifest_before" if is_before_block else "_manifest_after"
			if not hasattr(p, cache_field):
				manifest = set()
				for sub_packet in p:
					self._accumulate_entities(manifest, sub_packet, is_before_block)
				manifest.add(int(p.entity))
				setattr(p, cache_field, manifest)

			manifest = getattr(p, cache_field, set())
			accumulator.update(manifest)

		elif isinstance(p, packets.MetaData):
			accumulator.update(p.info)
		elif isinstance(p, packets.TagChange):
			if p.tag != GameTag.ZONE_POSITION:
				# Don't include entities in the manifest for snapshotting
				# just because their ZONE_POSITION changed.
				accumulator.add(p.entity)
		elif isinstance(p, (
			packets.FullEntity, packets.ShowEntity,
			packets.HideEntity, packets.ChangeEntity
		)):
			accumulator.add(p.entity)
		elif isinstance(p, packets.Choices):
			accumulator.add(p.entity)
			accumulator.add(int(p.source))
			accumulator.update(p.choices)
		elif isinstance(p, packets.ChosenEntities):
			accumulator.add(p.entity)
			accumulator.update(p.choices)
		elif isinstance(p, packets.SendChoices):
			accumulator.update(p.choices)

	def current_turn(self):
		return self.game.tags.get(GameTag.TURN, 0)

	def current_step(self):
		return self.game.tags.get(GameTag.STEP, 0)

	def record_zone_entrance(self, entity_id):
		entity = self.game.find_entity_by_id(entity_id)
		entity.entered_zone_on = self.current_turn()

	def get_block_records(self):
		if not self._game_info_is_set:
			raise RuntimeError("Must call set_game_info(...) before getting records.")
		return [{'Data': rec.to_record() + "\n"} for rec in self._block_records]

	def get_block_info_records(self):
		if not self._game_info_is_set:
			raise RuntimeError("Must call set_game_info(...) before getting records.")
		return [{'Data': rec.to_record() + "\n"} for rec in self._block_info_records]

	def get_entity_state_records(self):
		if not self._game_info_is_set:
			raise RuntimeError("Must call set_game_info(...) before getting records.")
		return [{'Data': rec.to_record() + "\n"} for rec in self._entity_state_records]

	def get_player_records(self):
		if not self._game_info_is_set:
			raise RuntimeError("Must call set_game_info(...) before getting records.")
		return [{'Data': rec.to_record() + "\n"} for rec in self._player_records]

	def get_game_records(self):
		if not self._game_info_is_set:
			raise RuntimeError("Must call set_game_info(...) before getting records.")
		return [{'Data': rec.to_record() + "\n"} for rec in self._game_records]

	def get_choice_records(self):
		if not self._game_info_is_set:
			raise RuntimeError("Must call set_game_info(...) before getting records.")
		return [{'Data': rec.to_record() + "\n"} for rec in self._choice_records]

	def get_option_records(self):
		if not self._game_info_is_set:
			raise RuntimeError("Must call set_game_info(...) before getting records.")
		return [{'Data': rec.to_record() + "\n"} for rec in self._option_records]
