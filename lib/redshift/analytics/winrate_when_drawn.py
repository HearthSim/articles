"""
A SQL Alchemy Expression Language implementation of the below SQL Query:

SELECT
	f_card_name(es_after.dbf_id) AS card,
	count(distinct b.id),
	(1.0 * sum(CASE WHEN es_after.controller_final_state = 4 THEN 1 ELSE 0 END)) / count(distinct b.id) AS win_rate_when_drawn
FROM block b
JOIN game g ON g.id = b.game_id
JOIN player p ON p.game_id = b.game_id AND b.entity_player_id = p.player_id
JOIN entity_state es_before ON es_before.before_block_id = b.id
JOIN entity_state es_after ON es_after.after_block_id = b.id
WHERE b.block_type = f_enum_val('BlockType.TRIGGER')
AND g.game_type = f_enum_val('BnetGameType.BGT_RANKED_STANDARD')
AND p.rank <= 10
AND b.step = f_enum_val('Step.MAIN_START')
AND es_after.dbf_id IS NOT NULL
AND es_before.entity_id = es_after.entity_id
AND es_before.zone = f_enum_val('Zone.DECK')
AND es_after.zone = f_enum_val('Zone.HAND')
GROUP BY es_after.dbf_id
HAVING count(*) >= 10
ORDER BY win_rate_when_drawn DESC;

This represents very initial prototyping work related to exposing a consistent set of Data Access APIs for Redshift.
"""
import os

from sqlalchemy import create_engine, case, between, distinct
from sqlalchemy.sql import func, select, desc
from sqlalchemy.sql import and_
from sqlalchemy import String
from hearthstone.enums import BlockType, BnetGameType, Step, Zone
from etl.models import block, game, player, entity_state

conn_info = os.environ.get("REDSHIFT_CONNECTION")
engine = create_engine(conn_info, echo=True)
conn = engine.connect()

# Create our aliases
es_before = entity_state.alias('es_before')
es_after = entity_state.alias('es_after')

# Create our columns
column_set = [
	func.f_card_name(es_after.c.dbf_id, type_=String).label('card'),
	func.count(distinct(block.c.id)),
	(func.sum(case([(es_after.c.controller_final_state == 4, 1)], else_=0)) * 1.0 / func.count(distinct(block.c.id))).label('win_rate_when_drawn')
]

# Create our join block
table_block = block.join(
	game
).join(
	player,
	(player.c.game_id == game.c.id) & (block.c.entity_player_id == player.c.player_id)
).join(
	es_before,
	es_before.c.before_block_id == block.c.id
).join(
	es_after,
	es_after.c.after_block_id == block.c.id
)

# Each of these represents an individual AND condition in our WHERE clause
is_trigger_block = block.c.block_type == BlockType.TRIGGER.value
is_game_type = game.c.game_type.in_([BnetGameType.BGT_RANKED_STANDARD.value])
in_rank_range = between(player.c.rank, 0, 10)
is_main_start = block.c.step == Step.MAIN_START.value
has_dbf_id = es_after.c.dbf_id != None
is_same_entity = es_before.c.entity_id == es_after.c.entity_id
in_deck_before = es_before.c.zone == Zone.DECK.value
in_hand_after = es_after.c.zone == Zone.HAND.value

# Create the WHERE clause
where_clause = and_(
	is_trigger_block,
	is_game_type,
	in_rank_range,
	is_main_start,
	has_dbf_id,
	is_same_entity,
	in_deck_before,
	in_hand_after
)

# Now composite the columns, the join_block, the WHERE clause, and the group bys into the final statement:
basic_stmt = select(column_set).select_from(table_block).where(where_clause)
grouped_stmt = basic_stmt.group_by(es_after.c.dbf_id).having(func.count(distinct(block.c.id)) >= 10)

# final_stmt is the final statement (surprise!)
final_stmt = grouped_stmt.order_by(desc('win_rate_when_drawn'))
print(final_stmt.compile(bind=engine))

# Run it and print the results
for row in conn.execute(final_stmt):
	print(row)
