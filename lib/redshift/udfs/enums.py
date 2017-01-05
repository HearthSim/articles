"""This will be packaged up alongside enums_data.py

See load_udfs_into_redshift.py for additional details.
"""
import json
from .enum_data import enum_data


_ENUM_DATA_CACHE = {}


def enums_json():
	if "enums_json" not in _ENUM_DATA_CACHE:
		_ENUM_DATA_CACHE["enums_json"] = json.loads(enum_data)
	return _ENUM_DATA_CACHE["enums_json"]


def enum_val(enum_name):
	if "." not in enum_name:
		raise Exception("Missing '.' delimiter. Expected input is <EnumClass>.<EnumMember>")

	enum_class_name, seperator, enum_member_name = enum_name.partition(".")

	if enum_class_name in enums_json():
		enum_class = enums_json()[enum_class_name]
		try:
			return enum_class[enum_member_name]
		except KeyError:
			raise Exception("%s is not a member of %s" % (enum_member_name, enum_class_name))
	else:
		raise Exception("%s is not a known enum class" % enum_class_name)


def enum_name(enum_class_name, enum_val):
	if enum_class_name in enums_json():
		enum_class = enums_json()[enum_class_name]
		for k,v in enum_class.items():
			if v == enum_val:
				return k
		raise Exception("No enum member matches value: %s" % enum_val)
	else:
		raise Exception("%s is not a known enum class" % enum_class_name)


