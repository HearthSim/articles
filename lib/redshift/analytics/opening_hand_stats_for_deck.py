"""
This query generates the win-rate for each card if it's in your opening hand broken out by the opponent's class. This
data can be useful for deciding which cards to mulligan for most aggressively based on your opponent.

E.g. (Worksheet 1)
https://docs.google.com/spreadsheets/d/1hcZvuBDNGPrwchDkk3-oOyeBUrqm6kBjt4WxaL3cssw/edit#gid=0

Avg Query Runtime: ~ 40 seconds


SELECT
	f_enum_name('CardClass', p_opponent.player_class) AS “opposing_class”,
	f_card_name(es.dbf_id) AS “card_name”,
	count(*) AS “num_games”,
	(1.0 * sum(CASE WHEN es.controller_final_state = 4 THEN 1 ELSE 0 END)) / count(*) AS “win_percentage”
FROM player p_deck
JOIN game g ON g.id = p_deck.game_id
JOIN player p_opponent ON p_deck.game_id = p_opponent.game_id
	AND p_opponent.player_id != p_deck.player_id
JOIN entity_state es ON es.game_id = p_deck.game_id AND es.controller = p_deck.player_id
WHERE p_deck.game_date = DATE '2016-12-17 00:00:00'
	AND p_deck.deck_id = 60963482
    AND g.game_type = 2
    AND p_deck.rank <= 10
	AND es.turn = 1
	AND es.step = f_enum_val('Step.MAIN_READY')
	AND es.zone = f_enum_val('Zone.HAND')
	AND es.dbf_id IS NOT NULL
GROUP BY p_opponent.player_class, es.dbf_id
HAVING count(*) >= 50
ORDER BY p_opponent.player_class, ((1.0 * sum(CASE WHEN es.controller_final_state = 4 THEN 1 ELSE 0 END)) / count(*)) DESC;

"""