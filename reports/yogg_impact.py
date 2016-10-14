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

	def handle_block(self, block):
		if block.type == BlockType.PLAY:
			entity = self.game.find_entity_by_id(block.entity)
			if entity.card_id == self.YOGG_SARON:
				self.handle_played_yogg(block, entity)
		super().handle_block(block)

	def is_block_type(self, block, block_type):
		return block.power_type == PowerType.BLOCK_START and block.type == block_type

	def get_played_sub_cards(self, block):
		for packet in block.packets:
			if self.is_block_type(packet, BlockType.POWER):
				for sub_packet in packet.packets:
					if self.is_block_type(sub_packet, BlockType.PLAY):
						entity = self.game.find_entity_by_id(sub_packet.entity)
						if entity.card_id:
							yield entity.card_id

	def handle_played_yogg(self, block, entity):
		controller = entity.controller
		if controller:
			turn = self.game.tags.get(GameTag.TURN)
			played_cards = self.get_played_sub_cards(block)
			self.events.append((controller.player_id, turn, played_cards))

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

		# This does not handle co-op or ties
		winner = player1.player_id if state1 == PlayState.WON else player2.player_id

		for player_id, turn, cards_played in exporter.events:
			turns_post_yogg = total_num_turns - turn
			result = "WON" if player_id == winner else "LOST"
			line = "%s,%s,%s" % (turns_post_yogg, "|".join(cards_played), result)

			yield None, line


if __name__ == "__main__":
	Job.run()
