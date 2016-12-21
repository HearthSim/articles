
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


stmt = select([
	func.f_card_name(block.c.entity_dbf_id),
	func.count(),
	(func.sum(case([(entity_state.c.controller_final_state == 4, 1)], else_=0)) * 1.0 / func.count()).label('win_rate')
]).select_from(
	block.join(
		entity_state, (entity_state.c.after_block_id == block.c.id) & (entity_state.c.entity_id == block.c.entity_id)
	).join(
		player, and_(player.c.game_id == block.c.game_id, player.c.player_id == block.c.entity_player_id)
	).join(
		game, game.c.id == block.c.game_id
	)
).where(and_(
	block.c.block_type == BlockType.PLAY.value,
	game.c.game_type == BnetGameType.BGT_RANKED_STANDARD.value,
	player.c.rank.between(0, 15)
)).group_by(
	block.c.entity_dbf_id
).having(
	func.count() >= 1000
).order_by(
	desc('win_rate')
)

print(stmt.compile(bind=engine))

# Run it and print the results
for row in conn.execute(stmt):
	print(row)
