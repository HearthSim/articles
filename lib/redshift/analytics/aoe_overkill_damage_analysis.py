"""
SELECT
b.id AS "block.id",
count(distinct bi.info_entity_id) AS "num_minions_hit",
sum(CASE WHEN (es_after.health - es_after.damage) < 0 THEN 1 ELSE 0 END) AS "num_minions_overkilled",
CASE WHEN sum(CASE WHEN (es_after.health - es_after.damage) < 0 THEN 1 ELSE 0 END) > 0 THEN
	round(((1.0 * sum(CASE WHEN (es_after.health - es_after.damage) < 0 THEN 1 ELSE 0 END)) / count(distinct bi.info_entity_id)) * 100, 2)
ELSE 0 END AS "percent_minions_overkilled",
sum(CASE WHEN (es_after.health - es_after.damage) < 0 THEN (es_after.health - es_after.damage) ELSE 0 END) AS "total_overkill_damage",
CASE WHEN sum(CASE WHEN (es_after.health - es_after.damage) < 0 THEN 1 ELSE 0 END) > 0 THEN
	round((-1.0 * sum(CASE WHEN (es_after.health - es_after.damage) < 0 THEN (es_after.health - es_after.damage) ELSE 0 END)) / sum(CASE WHEN (es_after.health - es_after.damage) < 0 THEN 1 ELSE 0 END), 2)
ELSE 0 END AS "avg_overkill_amount"
FROM block b
JOIN block_info bi ON bi.block_id = b.id
JOIN entity_state es_before ON es_before.before_block_id = b.id AND bi.info_entity_id = es_before.entity_id
JOIN entity_state es_after ON es_after.after_block_id = b.id AND bi.info_entity_id = es_after.entity_id
WHERE b.block_type = f_enum_val('BlockType.POWER')
AND b.entity_dbf_id = f_dbf_id('Hellfire')
AND bi.meta_data_type = f_enum_val('MetaDataType.DAMAGE')
GROUP BY b.id;

"""