#!/usr/bin/env python
"""
Produces a report of Yogg-Saron's impact in the meta by looking at the number
of turns between Yogg being played, the game ending and the game results.

Input: A sample of games in which Yogg-Saron may have been played (eg. in deck)
Format: Power.log
"""

from hearthstone.enums import GameTag, BlockType, PlayState, PowerType
from hearthstone.hslog.export import EntityTreeExporter
from mrjob.job import MRJob
from protocols import HSReplayS3Protocol

class YoggExporter(EntityTreeExporter):
	YOGG_SARON = "OG_134"

	def __init__(self, packet_tree):
		super().__init__(packet_tree)
		self.events = []
		self.played_cards = []
		self.playing_yogg = False
		self.play_block = (-1, -1)

	def handle_block(self, block):
		playing_yogg = False
		if block.type == BlockType.POWER:
			entity = self.game.find_entity_by_id(block.entity)
			if entity.card_id == self.YOGG_SARON:
				self.playing_yogg = playing_yogg = True
		elif block.type == BlockType.PLAY and self.playing_yogg:
			self.play_block = (block.entity, block.target)
		super().handle_block(block)
		if playing_yogg:
			entity = self.game.find_entity_by_id(block.entity)
			controller = entity.controller
			turn = self.game.tags.get(GameTag.TURN)
			self.events.append((controller.player_id, turn, self.played_cards))
			self.played_cards = []
			self.playing_yogg = False

	def handle_show_entity(self, block):
		super().handle_show_entity(block)
		if self.playing_yogg and block.entity == self.play_block[0]:
			card_info = self.get_card_info(*self.play_block)
			if card_info:
				self.played_cards.append(card_info)
			self.playing_entity = self.playing_target = -1

	def is_block_type(self, block, block_type):
		return block.power_type == PowerType.BLOCK_START and block.type == block_type

	def get_card_info(self, entity_id, target_id):
		entity = self.game.find_entity_by_id(entity_id)
		if entity.card_id:
			target = self.game.find_entity_by_id(target_id)
			target_card_id = target.card_id if target else ""
			is_friendly = target.controller.id == entity.controller.id if target else ""
			return "%s:%s:%s" % (entity.card_id, target_card_id, is_friendly)


class Job(MRJob):
	INPUT_PROTOCOL = HSReplayS3Protocol

	def mapper(self, line, replay):
		if not replay:
			return

		packet_tree = replay.to_packet_tree()[0]
		exporter = packet_tree.export(YoggExporter)
		game = exporter.game

		player1, player2 = game.players
		state1 = player1.tags.get(GameTag.PLAYSTATE, 0)

		total_num_turns = game.tags.get(GameTag.TURN, 0)

		winner_id = player1.player_id if state1 == PlayState.WON else player2.player_id

		for player_id, turn, cards_played in exporter.events:
			turns_post_yogg = total_num_turns - turn
			player = player1 if player_id == player1.player_id else player2
			opponent = player1 if player == player2 else player2
			player_hero = player.starting_hero.card_id
			opponent_hero = opponent.starting_hero.card_id
			player_health = player.hero.tags.get(GameTag.HEALTH, 0)
			player_damage = player.hero.tags.get(GameTag.DAMAGE, 0)
			player_hp = player_health - player_damage
			opp_health = opponent.hero.tags.get(GameTag.HEALTH, 0)
			opp_damage = opponent.hero.tags.get(GameTag.DAMAGE, 0)
			opponent_hp = opp_health - opp_damage
			won = "WON" if player_id == winner_id else "LOST"

			values = (
				player_hero, player_hp, opponent_hero, opponent_hp,
				won, turns_post_yogg, turn, "|".join(cards_played)
			)

			line = ",".join(["%s"] * len(values)) % values
			yield None, line


if __name__ == "__main__":
	Job.run()
