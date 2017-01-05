"""
A utility for deploying Python UDFs to Redshift.

For additional details, see:
http://docs.aws.amazon.com/redshift/latest/dg/user-defined-functions.html
"""
import os
import json
import requests
from pathlib import Path
from shutil import copyfile, make_archive
import boto3
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

S3 = boto3.client("s3")

ENUMS_JSON = "https://api.hearthstonejson.com/v1/enums.json"
CARDS_JSON = "https://api.hearthstonejson.com/v1/latest/enUS/cards.json"

BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..")

ENUMS_SOURCE = os.path.join(BASE_DIR, "lib", "redshift", "udfs", "enums.py")
CARDS_SOURCE = os.path.join(BASE_DIR, "lib", "redshift", "udfs", "cards.py")

ARTIFACTS_DIR = os.path.join(BASE_DIR, "build", "artifacts")
HEARTHSTONEJSON_PKG_DIR = os.path.join(ARTIFACTS_DIR, "hearthstonejson_pkg")
HEARTHSTONEJSON_MODULE_DIR = os.path.join(HEARTHSTONEJSON_PKG_DIR, "hearthstonejson")
ENUMS_DIR = os.path.join(HEARTHSTONEJSON_MODULE_DIR, "enums")
CARDS_DIR = os.path.join(HEARTHSTONEJSON_MODULE_DIR, "cards")

ENUMS_DEST = os.path.join(ENUMS_DIR, "__init__.py")
CARDS_DEST = os.path.join(CARDS_DIR, "__init__.py")

REDSHIFT_ARTIFACT_BUCKET = 'hsreplaynet-redshift-staging'

drop_hearthstonejson_cmd = "DROP LIBRARY hearthstonejson"

load_hearthstonejson_lib = """
CREATE LIBRARY hearthstonejson LANGUAGE plpythonu
FROM 's3://hsreplaynet-redshift-staging/libs/hearthstonejson.zip'
CREDENTIALS 'aws_access_key_id={ACCESS_KEY};aws_secret_access_key={SECRET_KEY}';
"""

# Converts an enum name to its integer representation, e.g. 'BlockType.PLAY' -> 7
f_enum_val = """
CREATE OR REPLACE FUNCTION f_enum_val(enum_name text) RETURNS integer IMMUTABLE as $$
	from hearthstonejson.enums import enum_val
	return enum_val(enum_name)
$$ LANGUAGE plpythonu;
"""

# Converts an integer to its enum member name, e.g. ('BlockType', 7) -> 'PLAY'
f_enum_name = """
CREATE OR REPLACE FUNCTION f_enum_name(enum_class_name text, enum_val integer) RETURNS text IMMUTABLE as $$
	from hearthstonejson.enums import enum_name
	return enum_name(enum_class_name, enum_val)
$$ LANGUAGE plpythonu;
"""

# Converts a card name to its DBF_ID
f_dbf_id = """
CREATE OR REPLACE FUNCTION f_dbf_id(card_name text) RETURNS integer IMMUTABLE as $$
	from hearthstonejson.cards import to_dbf_id
	return to_dbf_id(card_name)
$$ LANGUAGE plpythonu;
"""

# Converts a DBF_ID into a Card Name
f_card_name = """
CREATE OR REPLACE FUNCTION f_card_name(dbf_id integer) RETURNS text IMMUTABLE as $$
	from hearthstonejson.cards import to_card_name
	return to_card_name(dbf_id)
$$ LANGUAGE plpythonu;
"""

# Renders a human readable decklist from the JSON stored in the players table
f_pretty_decklist = """
CREATE OR REPLACE FUNCTION f_pretty_decklist(decklist varchar(max)) RETURNS varchar(max) IMMUTABLE as $$
	from hearthstonejson.cards import to_pretty_decklist
	return to_pretty_decklist(decklist)
$$ LANGUAGE plpythonu;
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


if __name__ == '__main__':
	# Make sure our artifact creation directories work
	os.makedirs(ENUMS_DIR, exist_ok=True)
	os.makedirs(CARDS_DIR, exist_ok=True)

	# Indicate the directories that are submodules
	Path(os.path.join(HEARTHSTONEJSON_PKG_DIR, '__init__.py')).touch()
	Path(os.path.join(HEARTHSTONEJSON_MODULE_DIR, '__init__.py')).touch()

	# Redshift UDFs cannot read anything except .py files, so we
	# wrap the JSON data in a string inside a .py file
	enums_data = requests.get(ENUMS_JSON).json()
	enum_data_path = os.path.join(ENUMS_DIR, "enum_data.py")
	with open(enum_data_path, "w") as out:
		out.write('enum_data = r"""\n')
		out.write(json.dumps(enums_data, indent=4))
		out.write('\n"""\n')

	copyfile(ENUMS_SOURCE, ENUMS_DEST)

	# Repeat the same process for wrapping the cards JSON in a .py file
	cards_data = requests.get(CARDS_JSON).json()
	cards_data_path = os.path.join(CARDS_DIR, "card_data.py")
	with open(cards_data_path, "w") as out:
		out.write('card_data = r"""\n')
		out.write(json.dumps(cards_data, indent=4))
		out.write('\n"""\n')

	copyfile(CARDS_SOURCE, CARDS_DEST)

	# Now build the expected .zip artifact
	make_archive(
		os.path.join(ARTIFACTS_DIR, 'hearthstonejson'),
		'zip',
		root_dir=HEARTHSTONEJSON_PKG_DIR,
	)

	# Upload the artifact to S3 where Redshift can reach it
	S3.put_object(
		Body=open(os.path.join(ARTIFACTS_DIR, 'hearthstonejson.zip'), 'rb'),
		Key='libs/hearthstonejson.zip',
		Bucket=REDSHIFT_ARTIFACT_BUCKET
	)

	conn_info = os.environ.get("REDSHIFT_CONNECTION")
	engine = create_engine(conn_info, echo=True)
	session = sessionmaker(bind=engine)()
	# We cannot be within a transaction when running commands like 'CREATE LIBRARY...'
	session.connection().connection.set_isolation_level(0)


	load_hearthstonejson_cmd = load_hearthstonejson_lib.format(
		ACCESS_KEY=os.environ.get("AWS_ACCESS_KEY_ID"),
		SECRET_KEY=os.environ.get("AWS_SECRET_ACCESS_KEY")
	)

	# First we must make sure the library is defined and tell Redshift where to find the source on S3
	# There is no "CREATE OR REPLACE..." for libraries so we must do it in two steps
	session.execute(drop_hearthstonejson_cmd)
	session.execute(load_hearthstonejson_cmd)

	# Finally we can start defining all our UDF functions
	session.execute(f_enum_val)
	session.execute(f_enum_name)
	session.execute(f_dbf_id)
	session.execute(f_card_name)
	session.execute(f_pretty_decklist)
	session.execute(f_player_turn)
