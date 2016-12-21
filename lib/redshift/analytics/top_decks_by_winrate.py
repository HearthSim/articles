"""

SELECT
	p.deck_id,
	count(*),
	(1.0 * sum(CASE WHEN final_state = 4 THEN 1 ELSE 0 END)) / count(*) AS win_rate
FROM player p
JOIN game g ON g.id = p.game_id
WHERE p.rank BETWEEN 0 AND 10
AND g.game_type = 2
AND p.full_deck_known
GROUP BY p.deck_id
HAVING count(*) >= 100
ORDER BY win_rate DESC;
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


stmt = select([
	player.c.deck_id,
	func.count(),
	(func.sum(case([(player.c.final_state == 4, 1)], else_=0)) * 1.0 / func.count()).label('win_rate')
]).select_from(
	player.join(game)
).where(and_(
	player.c.rank.between(0, 10),
	game.c.game_type == 2,
	player.c.full_deck_known,
)).group_by(
	player.c.deck_id
).having(
	func.count() >= 100
).order_by(
	desc('win_rate')
)

print(stmt.compile(bind=engine))

# Run it and print the results
for row in conn.execute(stmt):
	print(row)
