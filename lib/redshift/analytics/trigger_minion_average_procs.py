"""

SELECT avg(t.num_procs) AS “avg_num_cards_drawn”
FROM (
SELECT
	b.game_id,
	b.entity_id,
	count(distinct b_draw.id) AS num_procs
FROM block b
LEFT JOIN block b_draw ON b.game_id = b_draw.game_id AND b_draw.entity_id = b.entity_id AND b_draw.block_type = f_enum_val('BlockType.TRIGGER')
WHERE b.block_type = f_enum_val('BlockType.PLAY')
AND b.entity_dbf_id = f_dbf_id('Gadgetzan Auctioneer')
GROUP BY b.game_id, b.entity_id
) t;

"""