#!/usr/bin/env python
"""
Produces a report of Yogg-Saron's impact in the meta by looking at the number
of turns between Yogg being played, the game ending and the game results.

Input: A sample of games in which Yogg-Saron may have been played (eg. in deck)
Format: Power.log
"""

from collections import defaultdict

from hearthstone.enums import GameTag, BlockType, PlayState
from hearthstone.hslog.watcher import LogWatcher
from mrjob.job import MRJob

from mapred.protocols import PowerlogS3Protocol


class YoggEventWatcher(LogWatcher):
	YOGG_SARON = "OG_134"

	def __init__(self):
		super().__init__()
		self.yogg_events = []

	def on_block(self, action):
		if hasattr(action, "type") and action.type == BlockType.PLAY:
			if action.entity.card_id == self.YOGG_SARON:
				player = self.current_game.current_player.id
				turn = self.current_game.tags.get(GameTag.TURN, 0)
				self.yogg_events.append((player, turn))


class Job(MRJob):
	INPUT_PROTOCOL = PowerlogS3Protocol

	def mapper(self, line, log_fp):
		if not log_fp:
			return
		watcher = YoggEventWatcher()

		try:
			watcher.read(log_fp)
		except Exception as e:
			return

		packet_tree = watcher.games[0]
		game = packet_tree.game

		player1, player2 = game.players
		state1 = player1.tags[GameTag.PLAYSTATE]
		# state2 = player2.tags[GameTag.PLAYSTATE]

		total_num_turns = game.tags.get(GameTag.TURN, 0)

		# This does not handle co-op or ties
		winning_player_id = player1.id if state1 == PlayState.WON else player2.id

		for player_id_that_played_yogg, turn_yogg_played in watcher.yogg_events:
			num_turns_made_after_yogg = total_num_turns - turn_yogg_played
			yogg_controller_won = player_id_that_played_yogg == winning_player_id

			if yogg_controller_won:
				line = "%s,%s,1" % (num_turns_made_after_yogg, "YOGG_CONTROLLER_WON")
			else:
				line = "%s,%s,1" % (num_turns_made_after_yogg, "YOGG_CONTROLLER_LOST")

			yield None, line

	def combiner_init(self):
		self.DATA_SET = defaultdict(lambda: defaultdict(int))

	def combiner(self, key, yogg_events):
		for event_str in yogg_events:
			num_turns, outcome, count = event_str.split(",")
			self.DATA_SET[num_turns][outcome] += int(count)

	def combiner_final(self):
		for turn in sorted(self.DATA_SET.keys(), key=lambda k: int(k)):
			data = self.DATA_SET[turn]
			wins = [str(turn), "YOGG_CONTROLLER_WON", str(data["YOGG_CONTROLLER_WON"])]
			losses = [str(turn), "YOGG_CONTROLLER_LOST", str(data["YOGG_CONTROLLER_LOST"])]

			yield None, ",".join(wins)
			yield None, ",".join(losses)

	def reducer_init(self):
		self.DATA_SET = defaultdict(lambda: defaultdict(int))

	def reducer(self, key, yogg_events):
		for event_str in yogg_events:
			num_turns, outcome, count = event_str.split(",")
			self.DATA_SET[num_turns][outcome] += int(count)

	def reducer_final(self):
		COLUMNS = ["TURNS_SINCE_YOGG_PLAYED", "YOGG_CONTROLLER_WON", "YOGG_CONTROLLER_LOST"]
		yield None, ",".join(COLUMNS)

		for num_turns in sorted(self.DATA_SET.keys(), key=lambda k: int(k)):
			row_data = self.DATA_SET[num_turns]
			wins = row_data["YOGG_CONTROLLER_WON"]
			losses = row_data["YOGG_CONTROLLER_LOST"]

			row_result = [str(num_turns), str(wins), str(losses)]
			yield None, ",".join(row_result)


if __name__ == "__main__":
	Job.run()
