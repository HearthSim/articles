#!/usr/bin/env python
"""
Produces a CSV report on "A Friendly Game of Chess" Tavern Brawl.

Input: A sample of Chess Tavern Brawl games
Format: HSReplay XML
Output: CSV, multiple rows per game.
- Meta row: uuid,META,first_player,friendly_player,turns
- For every player:
uuid,PLAYER(1|2),hero,final_state,fatigue,hero_power_activations,drawn_cards,played_cards

drawn_cards and played_cards are pipe-separated card lists and follow this format:
cardID:turn|cardID:turn|...

Where `turn` is the turn the event happened (the turn the card was drawn or played on).
Note that `cardID` may be blank if the card was drawn but never played (never revealed).
"""

import csv
from io import StringIO
from uuid import uuid4
from hearthstone.enums import BlockType, GameTag, PlayState, Zone
from hearthstone.hslog.export import EntityTreeExporter, FriendlyPlayerExporter

from protocols import BaseJob


class DeepEntityTreeExporter(EntityTreeExporter):
	def handle_create_game(self, packet):
		super().handle_create_game(packet)
		for player in self.game.players:
			player.drawn_cards = []
			player.played_cards = []

	def handle_block(self, block):
		if block.type == BlockType.PLAY:
			entity = self.game.find_entity_by_id(block.entity)
			controller = entity.controller
			if controller:
				turn = self.game.tags.get(GameTag.TURN, 0)
				controller.played_cards.append((entity, turn))
		super().handle_block(block)

	def handle_drawn_card(self, entity_id):
		entity = self.game.find_entity_by_id(entity_id)
		if entity.zone == Zone.DECK:
			controller = entity.controller
			if controller:
				turn = self.game.tags.get(GameTag.TURN, 0)
				controller.drawn_cards.append((entity, turn))

	def handle_show_entity(self, packet):
		tags = dict(packet.tags)
		if tags.get(GameTag.ZONE, 0) == Zone.HAND:
			self.handle_drawn_card(packet.entity)
		super().handle_show_entity(packet)

	def handle_tag_change(self, packet):
		if packet.tag == GameTag.ZONE and packet.value == Zone.HAND:
			self.handle_drawn_card(packet.entity)
		super().handle_tag_change(packet)


def handle_replay(self, replay):
	packet_tree = replay.to_packet_tree()[0]
	exporter = packet_tree.export(DeepEntityTreeExporter)
	game = exporter.game

	id = uuid4()
	friendly_player = packet_tree.export(FriendlyPlayerExporter)
	first_player = game.first_player.player_id
	turns = game.tags[GameTag.TURN]

	out = StringIO()
	writer = csv.writer(out)

	meta_row = [
		id, "META", first_player, friendly_player, turns
	]
	writer.writerow(meta_row)

	for player in game.players:
		hero = player.starting_hero.card_id
		state = PlayState(player.tags.get(GameTag.PLAYSTATE, 0)).name
		fatigue = player.tags.get(GameTag.FATIGUE, 0)
		hero_power_activations = player.tags.get(GameTag.NUM_TIMES_HERO_POWER_USED_THIS_GAME, 0)
		drawn_cards = "|".join(
			"%s:%i" % (entity.card_id or "", turn) for entity, turn in player.drawn_cards
		)
		played_cards = "|".join(
			"%s:%i" % (entity.card_id or "", turn) for entity, turn in player.played_cards
		)
		row = [
			id, "PLAYER%i" % (player.player_id), hero, state, fatigue, hero_power_activations,
			drawn_cards, played_cards
		]
		writer.writerow(row)

	return out.getvalue().strip().replace("\r", "")


class Job(BaseJob):
	handler_function = handle_replay


if __name__ == "__main__":
	Job.run()
