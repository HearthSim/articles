"""
This query shows the cards presented in the initial phase of the mulligan, the % of times people keep it, and the
% of winning when it's in your opening hand.

E.g. (Worksheet 2)
https://docs.google.com/spreadsheets/d/1hcZvuBDNGPrwchDkk3-oOyeBUrqm6kBjt4WxaL3cssw/edit#gid=0

Average Query Runtime: 5 seconds


SELECT
	f_enum_name('CardClass', p_opponent.player_class) AS “opposing_class”,
	f_card_name(c.entity_dbf_id) AS "card_name",
	count(*) AS "times_presented_in_initial_cards",
	sum(CASE WHEN c.chosen THEN 1 ELSE 0 END) AS "times_kept",
    (1.0 * sum(CASE WHEN c.chosen THEN 1 ELSE 0 END)) / count(*) AS "keep_percentage",
	(1.0 * sum(CASE WHEN p_deck.final_state = 4 THEN 1 ELSE 0 END)) / count(*) AS “win_percentage”
FROM player p_deck
JOIN game g ON g.id = p_deck.game_id AND g.game_type = 2
JOIN player p_opponent ON p_deck.game_id = p_opponent.game_id
	AND p_opponent.player_id != p_deck.player_id
JOIN choices c ON c.game_id = p_deck.game_id
	AND c.player_entity_id = p_deck.entity_id
	AND c.choice_type = f_enum_val('ChoiceType.MULLIGAN')
    AND c.entity_dbf_id IS NOT NULL
WHERE p_deck.game_date = DATE '2016-12-17 00:00:00'
	AND p_deck.deck_id = 60963482
	AND p_deck.rank <= 10
GROUP BY p_opponent.player_class, c.entity_dbf_id
HAVING count(*) >= 50
ORDER BY p_opponent.player_class, ((1.0 * sum(CASE WHEN p_deck.final_state = 4 THEN 1 ELSE 0 END)) / count(*)) DESC;

"""