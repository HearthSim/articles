"""

This query takes a single card like 'Leeroy Jenkins' and calculates the average DAMAGE that it does for each instance
of it played, it also calculates the average winrate of the card's controller broken out by class


SELECT
	opponents_class,
	round(avg(t.corrected_total_damage),2) AS avg_damage,
	round(((1.0 * sum(controlling_player_won)) / count(*)) * 100, 2) AS win_rate
	FROM (
		SELECT
			f_enum_name('CardClass', targets_controller.player_class) AS opponents_class,
			b.game_id,
			b.entity_id,
			f_card_name(bi.info_entity_dbf_id),
			cast(sum(bi.data) AS numeric) AS total_damage,
			count(bi.block_id) AS num_duplicates,
			cast(sum(bi.data) AS numeric) / count(bi.block_id) AS corrected_total_damage,
			CASE WHEN max(es.controller_final_state) = 4 THEN 1 ELSE 0 END AS controlling_player_won
		FROM block b
		JOIN entity_state es ON es.before_block_id = b.id AND es.entity_id = b.entity_id
		JOIN game g ON g.id = b.game_id
		JOIN player p ON p.game_id = b.game_id AND b.entity_player_id = p.player_id
		JOIN block_info bi ON bi.block_id = b.id
		JOIN entity_state es_target ON es_target.before_block_id = b.id AND es_target.entity_id = bi.info_entity_id
		JOIN player targets_controller ON es_target.game_id = targets_controller.game_id
			AND targets_controller.player_id = es_target.controller
		WHERE b.entity_dbf_id = f_dbf_id('Leeroy Jenkins')
			AND p.player_class = f_enum_val('CardClass.WARLOCK')
			AND p.rank <= 10
			AND g.game_type = f_enum_val('BnetGameType.BGT_RANKED_STANDARD')
			AND bi.meta_data_type = f_enum_val('MetaDataType.DAMAGE')
			AND bi.info_entity_id != b.entity_id
		GROUP BY targets_controller.player_class, b.game_id, b.entity_id, bi.info_entity_dbf_id
		ORDER BY corrected_total_damage DESC
) t
GROUP BY opponents_class
ORDER BY win_Rate DESC;

"""

