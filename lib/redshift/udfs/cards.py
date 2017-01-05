"""This will be packaged up alongside cards_data.py

See load_udfs_into_redshift.py for additional details.
"""
import json
from .card_data import card_data

_CARD_DATA_CACHE = {}


def cards_json():
	if "cards_json" not in _CARD_DATA_CACHE:
		card_list = json.loads(card_data)
		_CARD_DATA_CACHE["cards_json"] = card_list
		_CARD_DATA_CACHE["dbf_to_card"] = {c['dbfId']: c for c in card_list if 'dbfId' in c}
		_CARD_DATA_CACHE["name_to_card"] = {c['name']: c for c in card_list if 'dbfId' in c}
	return _CARD_DATA_CACHE["cards_json"]


def dbf_to_card():
	if "dbf_to_card" not in _CARD_DATA_CACHE:
		cards_json()
	return _CARD_DATA_CACHE["dbf_to_card"]


def name_to_card():
	if "name_to_card" not in _CARD_DATA_CACHE:
		cards_json()
	return _CARD_DATA_CACHE["name_to_card"]


def to_dbf_id(card_name):
	db = name_to_card()
	if card_name in db:
		return db[card_name]['dbfId']
	else:
		return None


def sanitize_name(name):
	# Redshift chokes on the character \u2019 which is a back-tick glyph, as in C'Thun
	return name.replace(u'\u2019', u'\'').encode('ascii', 'ignore')


def to_card_name(dbf_id):
	db = dbf_to_card()
	if dbf_id in db:
		return sanitize_name(db[dbf_id]['name'])
	else:
		return None


def to_pretty_decklist(decklist_json_str):
	decklist = json.loads(decklist_json_str)
	db = dbf_to_card()
	cards = [(db[int(str(dbf_id))], count) for dbf_id, count in decklist.items()]
	alpha_sorted = sorted(cards, key=lambda t: t[0]['name'])
	mana_sorted = sorted(alpha_sorted, key=lambda t: t[0]['cost'])
	value_map = ["%s x %i" % (sanitize_name(card['name']), count) for card, count in mana_sorted]
	return "[%s]" % (", ".join(value_map))
