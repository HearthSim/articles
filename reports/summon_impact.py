#!/usr/bin/env python
"""
Produces a report for impact of summons for a given CARD_ID.

Input: A sample of games in which a card with summons was played.
Format: replay.xml
"""

from hearthstone.enums import CardType, GameTag, BlockType, PlayState, PowerType
from hearthstone.hslog.export import EntityTreeExporter
from mrjob.job import MRJob
from protocols import HSReplayS3Protocol


CARD_ID = "KAR_114" #Barns


def find_battlecry(block):
	for subblock in block.packets:
		if (
			subblock.power_type == PowerType.BLOCK_START and
			subblock.type == BlockType.POWER and
			subblock.entity == block.entity
		):
			return subblock


def find_summons(block):
	for packet in block.packets:
		if packet.power_type == PowerType.FULL_ENTITY:
			yield packet


class SummonExporter(EntityTreeExporter):

	def __init__(self, packet_tree):
		super().__init__(packet_tree)
		self.events = []

	def handle_block(self, block):
		if block.type == BlockType.PLAY:
			entity = self.game.find_entity_by_id(block.entity)
			if entity.card_id == CARD_ID:
				self.handle_card_played(block, entity)
		super().handle_block(block)

	def filter_minion(self, packet):
		entity = self.game.find_entity_by_id(packet.entity)
		return entity.tags.get(GameTag.CARDTYPE) == CardType.MINION

	def handle_card_played(self, block, entity):
		controller = entity.controller
		if controller:
			battlecry = find_battlecry(block)
			summons = filter(self.filter_minion, list(find_summons(battlecry)))
			if summons:
				summon_ids = map(lambda entity: entity.card_id, summons)
				turn = self.game.tags.get(GameTag.TURN)
				self.events.append((controller.player_id, turn, summon_ids))

class Job(MRJob):
	INPUT_PROTOCOL = HSReplayS3Protocol

	def mapper(self, line, replay):
		if not replay:
			return

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
			player_hero = game.get_player(player_id).starting_hero.card_id[0:7]
			opponent_hero = game.get_player(opponent_id).starting_hero.card_id[0:7]
			region = player1.account_hi
			values = (
				player_hero, opponent_hero, won, first_player,
				turn, total_turns, "|".join(summons), region
			)
			line = ",".join(["%s"] * len(values)) % values
			yield None, line

if __name__ == "__main__":
	Job.run()
