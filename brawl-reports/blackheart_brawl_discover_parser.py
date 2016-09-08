#!/usr/bin/env python

import boto3
import csv
import gzip
import sys
from uuid import uuid4
from io import StringIO
from hearthstone.enums import ChoiceType, GameTag
from hearthstone.hslog.watcher import LogWatcher


S3 = boto3.client("s3")
BUCKET = "hsreplaynet-replays"


class CustomWatcher(LogWatcher):
	def __init__(self):
		super().__init__()
		self.tb_choices = []

	def on_choice_sent(self, player, choice):
		c = self.choices[choice.id]
		if c.type == ChoiceType.MULLIGAN:
			return
		source = c.source.card_id
		if source != "TB_013":
			return
		if not c.choices[0].card_id:
			# choice is not revealed - skip
			return
		assert len(choice.choices) == 1
		choices = [k.card_id for k in c.choices]
		picked = choice.choices[0].card_id
		turn = self.current_game.tags.get(GameTag.TURN, 0)
		if isinstance(player, str):
			print("WARNING: Cannot get friendly player", file=sys.stderr)
			health, damage, armor = "", "", ""
		else:
			health = player.hero.tags.get(GameTag.HEALTH, 0)
			damage = player.hero.tags.get(GameTag.DAMAGE, 0)
			armor = player.hero.tags.get(GameTag.ARMOR, 0)
		self.tb_choices.append([turn, health, damage, armor, picked] + choices)


def parse_file(f):
	watcher = CustomWatcher()
	watcher.read(f)

	uuid = uuid4()
	packet_tree = watcher.games[0]
	game = packet_tree.game
	friendly_player = packet_tree.guess_friendly_player()
	first_player = game.first_player.player_id
	player1, player2 = game.players
	hero1 = player1.starting_hero.card_id
	hero2 = player2.starting_hero.card_id
	state1 = player1.tags.get(GameTag.STATE, 0)
	state2 = player2.tags.get(GameTag.STATE, 0)

	out = StringIO()
	writer = csv.writer(out)

	metadata = [uuid, first_player, friendly_player, hero1, hero2, state1, state2]

	for values in watcher.tb_choices:
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


def main():
	# do_paths(sys.argv[1:])
	with open(sys.argv[1], "r") as f:
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


if __name__ == "__main__":
	main()
