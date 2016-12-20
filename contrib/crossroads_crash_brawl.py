#!/usr/bin/env python
"""
Produces a CSV report of the "Cart Crash at the Crossroads" Tavern Brawl.

Input: A sample of games playing Cart Crash at the Crossroads (Scenario 1812)
Format: Power.log
Output: CSV, one row per game - Each line looks like the following:
	first_player,friendly_player,hero1,hero2,final_state1,final_state2,turns,
	picked1,choice1_1,choice1_2,choice1_3,picked2,choice2_1,choice2_2,choice2_3

NOTE: Two of the opponent's choices will be empty as there is no way to know them.
"""

import csv
from io import StringIO

from hearthstone.enums import ChoiceType, GameTag
from hearthstone.hslog.watcher import LogWatcher
from mrjob.job import MRJob

from mapred.protocols import PowerlogS3Protocol


class CustomWatcher(LogWatcher):
	def __init__(self):
		super().__init__()
		self.pick_entities = []

	def on_choice_sent(self, player, choice):
		# Whenever a choice pick is sent, get the source choice
		assert choice.id in self.choices, "Cannot find source choice"
		c = self.choices[choice.id]
		if c.type == ChoiceType.MULLIGAN:
			# discard mulligans
			return

		source = c.source.card_id
		if source != "TB_ClassRandom_PickSecondClass":
			# discard anything that isn't from the TB source
			return

		# Try get the card_id. It might not be available yet though.
		# If it's not available, fill in the entity id, we'll replace it later.
		assert len(choice.choices) == 1
		choices = [k.card_id or k.id for k in c.choices]
		assert len(choices) == 3
		picked = choice.choices[0]
		picked = picked.card_id or picked.id

		self.pick_entities.append([picked] + choices)


def parse_file(f):
	watcher = CustomWatcher()
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

	out = StringIO()
	writer = csv.writer(out)

	metadata = [first_player, friendly_player, hero1, hero2, state1.name, state2.name, turns]

	assert len(watcher.pick_entities) == 2
	values = watcher.pick_entities[0] + watcher.pick_entities[1]
	for i, value in enumerate(values):
		if isinstance(value, int):
			# We have an entity ID instead of a card id; find the true id and replace it.
			# Results in an empty string if not available.
			values[i] = game.find_entity_by_id(value).card_id

	writer.writerow(metadata + values)

	return out.getvalue().strip().replace("\r", "")


class Job(MRJob):
	INPUT_PROTOCOL = PowerlogS3Protocol

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
