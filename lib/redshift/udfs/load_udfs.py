""" Work In Progress

The goal of this script is to package up the external libraries we would like to use within python UDF scripts
in Redshift, then upload them to S3, connect to Redshift and make them available for use.

Each of the UDFs is defined below. The libraries that they reference must be zipped up, copied to S3, and then imported
into Redshift. The command to import a library from S3 into Redshift is as follows:

CREATE LIBRARY enums34 LANGUAGE plpythonu
FROM 's3://hsreplaynet-redshift-staging/udfs/enums34.zip'
CREDENTIALS  'aws_access_key_id=<ACCESS_KEY>;aws_secret_access_key=<SECRET_KEY>';

The zip file can be generated with a command like:

$ find . -name "*.py" -print  | zip enums34.zip -@

Note, Python UDFs in Redshift run under Python 2.7, because python-hearthstone uses the IntEnum class from Python 3.4
we must first add the enums34 library to Redshift before we can import hearthstone.enums.

Reference:
http://docs.aws.amazon.com/redshift/latest/dg/user-defined-functions.html

TODO: We need to automate:
	- Checking out enums34
	- Packing it up and uploading it to S3
	- Checking out python-hearthstone
	- Inlining the CardDefs.xml data (see the docstring in cards.py for more details)
	- Packing it up and uploading it to S3
	- Defining the libraries in Redshift via the `CREATE LIBRARY` syntax
	- Creating the UDFs in Redshift via the below `CREATE OR REPLACE FUNCTION` syntax
"""


# When players have 5 mana they think they are on turn 5 but the TURN tag in the game
# will be at 9 or 10 at that point because the game increments the TURN tag each time the controller
# changes. This helper function is a convenience to translate values in the DB to player
# expected values.
f_player_turn = """
CREATE OR REPLACE FUNCTION f_player_turn(turn integer) RETURNS integer IMMUTABLE as $$
	return round((1.0 * turn) / 2)
$$ LANGUAGE plpythonu;
"""

# Converts a card name to it's DBF_ID
f_dbf_id = """
CREATE OR REPLACE FUNCTION f_dbf_id(card_name text) RETURNS integer IMMUTABLE as $$
	from hearthstone import cards
	db = cards.name_to_dbf_id()
	if card_name in db:
		return int(db[card_name])
	else:
		return None
$$ LANGUAGE plpythonu;
"""

# Converts a DBF_ID into a Card Name
f_card_name = """
CREATE OR REPLACE FUNCTION f_card_name(dbf_id integer) RETURNS text IMMUTABLE as $$
	from hearthstone import cards
	key = str(dbf_id)
	db = cards.dbf_id_to_name()
	if key in db:
		return db[key].replace(u'\u2019', u'\'').encode('ascii', 'ignore')
	else:
		return None
$$ LANGUAGE plpythonu;
"""

# Converts an enum name from `hearthstone.enums.*` into it's integer representation
f_enum_val = """
CREATE OR REPLACE FUNCTION f_enum_val(enum_name text) RETURNS integer IMMUTABLE as $$
	from hearthstone import enums
	if "." not in enum_name:
		raise Exception("Missing '.' delimiter. Expected input is <EnumClass>.<EnumMember>")
	enum_class_name, seperator, enum_member_name = enum_name.partition(".")
	if hasattr(enums, enum_class_name):
		enum_class = getattr(enums, enum_class_name)
		try:
			enum_member = enum_class[enum_member_name]
			return enum_member.value
		except KeyError:
			raise Exception("%s is not a member of hearthstone.enums.%s" % (enum_member_name,enum_class_name))
	else:
		raise Exception("%s is not a class in hearthstone.enums" % enum_class_name)
$$ LANGUAGE plpythonu;
"""

# Converts an integer to it's enum member from a named `hearthstone.enum.*` class
f_enum_name = """
CREATE OR REPLACE FUNCTION f_enum_name(enum_class_name text, enum_val integer) RETURNS text IMMUTABLE as $$
	from hearthstone import enums
	if hasattr(enums, enum_class_name):
		enum_class = getattr(enums, enum_class_name)
		enum_member = enum_class(enum_val)
		return enum_member.name
	else:
		raise Exception("%s is not a class in hearthstone.enums" % enum_class_name)
$$ LANGUAGE plpythonu;
"""
