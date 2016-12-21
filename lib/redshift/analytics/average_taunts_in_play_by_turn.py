"""


SELECT
	t.turn,
	sum(t.num_taunts_in_play) AS "total_taunts_in_play",
	count(*) AS "num_turns_evaluated",
	(1.0 * sum(t.num_taunts_in_play)) / count(*) AS "probability_of_a taunt_in_play"
FROM (
SELECT
	b.game_id,
	b.entity_id,
	f_player_turn(b.turn) AS "turn",
	sum(CASE WHEN es_before.taunt = True THEN 1 ELSE 0 END) AS "num_taunts_in_play"
FROM block b
LEFT JOIN entity_state es_before ON es_before.before_block_id = b.id AND es_before.zone = f_enum_val('Zone.PLAY')
WHERE b.block_type = f_enum_val('BlockType.TRIGGER')
AND b.step = f_enum_val('Step.MAIN_START')
AND b.entity_id IN (2, 3)
GROUP BY b.game_id, b.entity_id, b.turn
) t
GROUP BY t.turn
ORDER BY t.turn;

"""