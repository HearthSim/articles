#!/usr/bin/env python
"""
Produces a CSV report on Tuskarr Totemic, looking at games where Tuskarr Totemic
was played before player-turn 4, what it summoned, and what the result of the game was.

The parser works by hooking into the PLAY block.
- If game turn is < 7, look at the cardID
- If the cardID is AT_046 (Tuskarr Totemic), try to find the matching summoned totem
- Record the controller, the play and the summoned totem.

Input: A sample of games where Tuskarr Totemic was potentially played.
Format: Power.log
Output: CSV, multiple rows per game.
- Meta row: uuid,first_player,friendly_player,hero1,hero2,final_state1,final_state2,turns
- For every tuskarr totemic played: uuid,turn,current_player,controller,summoned_totem
"""

import csv
from io import StringIO
from uuid import uuid4

from hearthstone.enums import BlockType, GameTag, PowerType
from hearthstone.hslog.watcher import LogWatcher
from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol

from mapred.protocols import PowerlogS3Protocol

TUSKARR_TOTEMIC = "AT_046"


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
			yield packet.cardid


class TuskarrTotemicLogWatcher(LogWatcher):
	def __init__(self):
		super().__init__()
		self.tuskarrs = []

	def on_block(self, block):
		turn = self.current_game.tags.get(GameTag.TURN, 0)
		if getattr(block, "type", 0) == BlockType.PLAY:
			if block.entity.card_id == TUSKARR_TOTEMIC:
				current_player = self.current_game.current_player.player_id
				controller = block.entity.tags.get(GameTag.CONTROLLER, 0)
				battlecry = find_battlecry(block)
				summons = list(find_summons(battlecry))
				if summons:
					summoned_totem = summons[0]
				else:
					summoned_totem = ""

				ret = turn, current_player, controller, summoned_totem
				self.tuskarrs.append(ret)


def parse_file(f):
	watcher = TuskarrTotemicLogWatcher()
	watcher.read(f)

	packet_tree = watcher.games[0]
	game = packet_tree.game

	id = uuid4()
	friendly_player = packet_tree.guess_friendly_player()
	first_player = game.first_player.player_id
	player1, player2 = game.players
	hero1 = player1.starting_hero.card_id
	hero2 = player2.starting_hero.card_id
	state1 = player1.tags[GameTag.PLAYSTATE]
	state2 = player2.tags[GameTag.PLAYSTATE]
	turns = game.tags[GameTag.TURN]

	if not watcher.tuskarrs:
		# Uninteresting game. Nothing to return.
		return ""

	out = StringIO()
	writer = csv.writer(out)

	row = [
		id, "META", first_player, friendly_player,
		hero1, hero2, state1.name, state2.name, turns
	]
	writer.writerow(row)
	for turn, current_player, controller, summoned_totem in watcher.tuskarrs:
		writer.writerow([id, "ACTION", turn, current_player, controller, summoned_totem])

	return out.getvalue().strip().replace("\r", "")


class Job(MRJob):
	INPUT_PROTOCOL = PowerlogS3Protocol
	OUTPUT_PROTOCOL = RawValueProtocol

	def mapper(self, line, log_fp):
		if not log_fp:
			return

		try:
			value = parse_file(log_fp)
		except Exception as e:
			return

		self.increment_counter("replays", "replays_processed")
		yield None, value


if __name__ == "__main__":
	Job.run()
