""" Work In Progress!

The goal of this script is to be able to generate a '|' delimited text file that represents the enUS
Card DB, then upload it to S3, connect to Redshift and truncate and then repopulate a card table based on CardDefs.xml

"""
from pkg_resources import resource_filename
from xml.etree import ElementTree
from hearthstone.cardxml import CardXML
from hearthstone.enums import (
	CardClass, CardType, CardSet, Faction, Race, Rarity, GameTag, PlayReq
)


if __name__ == '__main__':
	path = resource_filename("hearthstone", "CardDefs.xml")

	db = {}
	output = open("cards_output.data", "w")

	is_first = True

	with open(path, "r", encoding="utf8") as f:
		xml = ElementTree.parse(f)
		for carddata in xml.findall("Entity"):
			card = CardXML("enUS")
			card.load_xml(carddata)
			db[card.id] = card

			if is_first:
				pass
				is_first = False

			cols = [str(getattr(card, k)) for k in dir(card) if not k.startswith("_")]
			row = "|".join(cols) + "\n"
			output.write(row)
