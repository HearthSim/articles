import os
import requests
from hearthstone.enums import CardClass, CardType, CardSet, Faction, Race, Rarity
from sqlalchemy import create_engine, select
from redshift.etl.models import card


CARDS_JSON = "https://api.hearthstonejson.com/v1/latest/enUS/cards.json"


if __name__ == '__main__':
	conn_info = os.environ.get("REDSHIFT_CONNECTION")
	engine = create_engine(conn_info, echo=True)
	conn = engine.connect()

	s = select([card])
	result = conn.execute(s)
	existing_dbf_ids = set()
	for row in result:
		existing_dbf_ids.add(row['dbf_id'])

	print("Existing DBF IDs: %s" % str(existing_dbf_ids))
	inserts = []

	print("\n\n***\nExisting Card Count: %s" % len(existing_dbf_ids))
	cards = requests.get(CARDS_JSON).json()
	print("Total JSON Card Count: %s" % len(cards))
	for card_data in cards:
		if 'dbfId' in card_data and card_data['dbfId'] not in existing_dbf_ids:
			ins_values = dict(
				id=card_data['id'],
				dbf_id=card_data['dbfId'],
				name=card_data['name'],
				card_class=CardClass[card_data.get('playerClass', 'INVALID')].value,
				card_set=CardSet[card_data.get('set', 'INVALID')].value,
				faction=Faction[card_data.get('faction', 'INVALID')].value,
				race=Race[card_data.get('race', 'INVALID')].value,
				rarity=Rarity[card_data.get('rarity', 'INVALID')].value,
				type=CardType[card_data.get('type', 'INVALID')].value,
				collectible=card_data.get('collectible', False),
				battlecry='BATTLECRY' in card_data.get('mechanics', []),
				divine_shield='DIVINE_SHIELD' in card_data.get('mechanics', []),
				deathrattle='DEATHRATTLE' in card_data.get('mechanics', []),
				elite=card_data.get('rarity', 'INVALID') == 'LEGENDARY',
				evil_glow='EVIL_GLOW' in card_data.get('mechanics', []),
				inspire='INSPIRE' in card_data.get('mechanics', []),
				forgetful='FORGETFUL' in card_data.get('mechanics', []),
				one_turn_effect='TAG_ONE_TURN_EFFECT' in card_data.get('mechanics', []),
				poisonous='POISONOUS' in card_data.get('mechanics', []),
				ritual='RITUAL' in card_data.get('mechanics', []),
				secret='SECRET' in card_data.get('mechanics', []),
				taunt='TAUNT' in card_data.get('mechanics', []),
				topdeck='TOPDECK' in card_data.get('mechanics', []),
				atk=card_data.get('attack', 0),
				health=card_data.get('health',0),
				durability=card_data.get('durability', 0),
				cost=card_data.get('cost', 0),
				windfury=1 if 'WINDFURY' in card_data.get('mechanics', []) else 0,
				overload=card_data.get('overload', 0)
			)
			inserts.append(ins_values)
	print("Pending Insert Count: %s" % len(inserts))
	conn.execute(card.insert(), inserts)
