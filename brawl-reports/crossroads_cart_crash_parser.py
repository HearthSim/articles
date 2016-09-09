#!/usr/bin/env python

import boto3
import csv
import gzip
import sys
from io import StringIO
from hearthstone.enums import ChoiceType, GameTag
from hearthstone.hslog.watcher import LogWatcher


S3 = boto3.client("s3")
BUCKET = "hsreplaynet-replays"
COLUMNS = [
	"First player", "Friendly player",
	"Player1 Hero", "Player2 Hero",
	"Player1 Playstate", "Player2 Playstate",
	"FirstPlayer Pick", "FirstPlayer Choice1", "FirstPlayer Choice2", "FirstPlayer Choice3",
	"SecPlayer Pick", "SecPlayer Choice1", "SecPlayer Choice2", "SecPlayer Choice3",
]


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

	out = StringIO()
	writer = csv.writer(out)

	metadata = [first_player, friendly_player, hero1, hero2, state1.name, state2.name]

	assert len(watcher.pick_entities) == 2
	values = watcher.pick_entities[0] + watcher.pick_entities[1]
	for i, value in enumerate(values):
		if isinstance(value, int):
			# We have an entity ID instead of a card id; find the true id and replace it.
			# Results in an empty string if not available.
			values[i] = game.find_entity_by_id(value).card_id

	# Each row looks like:
	# first_player,friendly_player,hero1,hero2,final_state1,final_state2,
	# picked1,choice1_1,choice1_2,choice1_3,picked2,choice2_1,choice2_2,choice2_3
	# Two of the opponent's choices will be empty as there is no way to know them.
	# For this brawl, it's one row per game.
	writer.writerow(metadata + values)

	return out.getvalue().strip().replace("\r", "")


def do_paths(args):
	for path in args:
		with open(path, "r") as f:
			out = parse_file(f)
			print(out)


def do_s3(bucket, key):
	obj = S3.get_object(Bucket=bucket, Key=key)
	gzstream = obj["Body"].read()
	ret = gzip.decompress(gzstream).decode("utf-8")
	f = StringIO(ret)
	return parse_file(f)


def do_s3_list(filename):
	with open(filename, "r") as f:
		lines = f.read().split()

	logs = [line.strip() for line in lines if line]
	total = len(logs)
	for i, key in enumerate(logs):
		print("%i / %i - %s" % (i + 1, total, key), file=sys.stderr)
		if key:
			try:
				print(do_s3(BUCKET, key))
			except Exception as e:
				print("ERROR: Cannot parse %r: %r" % (key, e), file=sys.stderr)


def main():
	# do_paths(sys.argv[1:])

	print(",".join(COLUMNS))

	filename = sys.argv[1]
	if filename.endswith(".log"):
		do_paths(sys.argv[1:])
	else:
		# Generate list with:
		# GlobalGame.objects.filter(scenario_id=1812).values_list("replays__uploads__file")
		do_s3_list(filename)


if __name__ == "__main__":
	main()
