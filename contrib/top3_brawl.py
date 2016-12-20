#!/usr/bin/env python
"""
Produces a CSV report of the "Top 3" Tavern Brawl.

Input: A sample of games playing Top 3 (Scenario 1739)
Format: Power.log
Output: CSV, one row per game - Each line looks like the following:
	first_player,friendly_player,hero1,hero2,final_state1,final_state2,turns,fatigue1,fatigue2,deck1,deck2

deck1 and deck2 are pipe-separated (`|`) unique sets of 0 or more card IDs.
The card IDs are found by looking at the final list of entities in the game and
finding all the revealed IDs, then filtering for all those without a CREATOR.
For performance reasons, the COLLECTIBLE value is not checked as it requires
access to the CardXML database - that means you may find more than three values
in the final set. You should do a second pass on it, filtering for COLLECTIBLE.
"""

import csv
from io import StringIO

from hearthstone.enums import GameTag
from hearthstone.hslog.watcher import LogWatcher
from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol

from mapred.protocols import PowerlogS3Protocol


def parse_file(f):
	watcher = LogWatcher()
	watcher.read(f)

	packet_tree = watcher.games[0]
	game = packet_tree.game

	friendly_player = packet_tree.guess_friendly_player()
	first_player = game.first_player.player_id
	player1, player2 = game.players
	hero1 = player1.starting_hero.card_id
	hero2 = player2.starting_hero.card_id
	state1 = player1.tags[GameTag.PLAYSTATE]
	state2 = player2.tags[GameTag.PLAYSTATE]
	turns = game.tags[GameTag.TURN]
	fatigue1 = player1.tags.get(GameTag.FATIGUE, 0)
	fatigue2 = player2.tags.get(GameTag.FATIGUE, 0)

	decks = {
		1: set(),
		2: set(),
	}

	for player in game.players:
		for entity in player.initial_deck:
			if entity.card_id:
				decks[player.player_id].add(entity.card_id)

	deck1 = "|".join(sorted(decks[1]))
	deck2 = "|".join(sorted(decks[2]))

	out = StringIO()
	writer = csv.writer(out)

	row = [
		first_player, friendly_player, hero1, hero2, state1.name, state2.name, turns,
		fatigue1, fatigue2, deck1, deck2
	]
	writer.writerow(row)

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
