#!/usr/bin/env python
"""
Produces a report for impact of summons for a given CARD_ID.

Input: A sample of games in which a card with summons was played.
Format: replay.xml
"""

from hearthstone.enums import CardType, GameTag, BlockType, PlayState
from hearthstone.hslog.export import EntityTreeExporter
from mrjob.job import MRJob
from protocols import HSReplayS3Protocol


CARD_ID = "NEW1_031" #Animal Companion


def is_minion(entity):
	return entity.tags.get(GameTag.CARDTYPE, 0) == CardType.MINION


class SummonExporter(EntityTreeExporter):

	def __init__(self, packet_tree):
		super().__init__(packet_tree)
		self.events = []
		self.summons = []
		self.entity = None
		self.waiting_for_entity = -1
		self.waiting_for_summons = False

	def handle_block(self, block):
		if block.type == BlockType.PLAY:
			self.handle_play_block(block)
		elif block.type == BlockType.POWER and self.entity is not None:
			self.waiting_for_summons = True
		super().handle_block(block)
		if block.type == BlockType.PLAY:
			self.handle_play_block_end()
		elif block.type == BlockType.POWER:
			self.waiting_for_summons = False

	def handle_show_entity(self, block):
		super().handle_show_entity(block)
		if block.entity == self.waiting_for_entity:
			entity = self.game.find_entity_by_id(block.entity)
			if entity and entity.card_id == CARD_ID:
				self.entity = entity

	def handle_full_entity(self, block):
		super().handle_full_entity(block)
		if self.waiting_for_summons:
			entity = self.game.find_entity_by_id(block.entity)
			self.summons.append(entity)

	def handle_play_block(self, block):
		entity = self.game.find_entity_by_id(block.entity)
		if not entity.card_id:
			self.waiting_for_entity = block.entity
		elif entity.card_id == CARD_ID:
			self.entity = entity

	def handle_play_block_end(self):
		if self.entity is not None and self.entity.card_id:
			summons = list(filter(is_minion, self.summons))
			if summons:
				controller = self.entity.controller
				summon_ids = map(lambda entity: entity.card_id, summons)
				turn = self.game.tags.get(GameTag.TURN)
				self.events.append((controller.player_id, turn, summon_ids))
		self.summons = []
		self.entity = None
		self.waiting_for_entity = -1


def get_deck_content(player):
	if not player or not player.initial_deck:
		return ""
	cards = filter(lambda x: x.card_id, player.initial_deck)
	card_ids = map(lambda x: x.card_id, cards)
	return "|".join(card_ids)


class Job(MRJob):
	INPUT_PROTOCOL = HSReplayS3Protocol

	def mapper(self, _, replay):
		if not replay:
			return
		for data in self.analyze_replay(replay):
			yield None, data

	def analyze_replay(self, replay):
		packet_tree = replay.to_packet_tree()[0]
		exporter = packet_tree.export(SummonExporter)
		game = exporter.game

		first_player = game.first_player.player_id
		player1, player2 = game.players
		state1 = player1.tags[GameTag.PLAYSTATE]
		total_turns = game.tags[GameTag.TURN]
		winner = player1 if state1 == PlayState.WON else player2

		for player_id, turn, summons in exporter.events:
			won = player_id == winner.player_id
			first_player = player_id == first_player
			opponent_id = player_id % 2 + 1
			player = game.get_player(player_id)
			opponent = game.get_player(opponent_id)
			player_hero = player.starting_hero.card_id[0:7]
			player_deck = get_deck_content(player)
			opponent_hero = opponent.starting_hero.card_id[0:7]
			opponent_deck = get_deck_content(opponent)
			region = player1.account_hi
			values = (
				player_hero, opponent_hero, won, first_player,
				turn, total_turns, "|".join(summons), region,
				player_deck, opponent_deck
			)

			yield ",".join(["%s"] * len(values)) % values


if __name__ == "__main__":
	Job.run()
