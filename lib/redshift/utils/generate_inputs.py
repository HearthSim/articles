"""
A command line utility for generating input.txt files in the format expected by the `load_redshift.py` job.
"""
import os
import json
import time
import argparse
from datetime import datetime, timedelta
import psycopg2


TEMPLATE = """
SELECT
    gg.id AS game_id,
    max(gr.replay_xml) AS replay_xml,
    gg.game_type,
    gg.scenario_id,
    gg.ladder_season,
    gg.brawl_season,
    max(ggp1.deck_list_id) AS player1_deck_id,
    (
        SELECT '{' || string_agg('"' || c1.dbf_id || '":' || ci1.count, ',') || '}'
        FROM cards_include ci1 JOIN card c1 ON c1.id = ci1.card_id
        WHERE ci1.deck_id = max(ggp1.deck_list_id)
    ) AS player1_deck_list,
    max(cd1.archetype_id) AS player1_archetype_id,
    CASE WHEN max(cd1.size) = 30 THEN TRUE ELSE FALSE END AS player1_full_deck_known,
    CASE WHEN max(ggp1.legend_rank) IS NOT NULL THEN 0 ELSE max(ggp1.rank) END AS player1_rank,
    max(ggp1.legend_rank) AS player1_legend_rank,
    max(ggp2.deck_list_id) AS player2_deck_id,
    (
        SELECT '{' || string_agg('"' || c2.dbf_id || '":' || ci2.count, ',') || '}'
        FROM cards_include ci2 JOIN card c2 ON c2.id = ci2.card_id
        WHERE ci2.deck_id = max(ggp2.deck_list_id)
    ) AS player2_deck_list,
    max(cd2.archetype_id) AS player2_archetype_id,
    CASE WHEN max(cd2.size) = 30 THEN TRUE ELSE FALSE END AS player2_full_deck_known,
    CASE WHEN max(ggp2.legend_rank) IS NOT NULL THEN 0 ELSE max(ggp2.rank) END AS player2_rank,
    max(ggp2.legend_rank) AS player2_legend_rank
FROM games_globalgame gg
JOIN games_gamereplay gr ON gr.global_game_id = gg.id
JOIN games_globalgameplayer ggp1 ON ggp1.game_id = gg.id AND ggp1.player_id = 1
JOIN cards_deck cd1 ON cd1.id = ggp1.deck_list_id
JOIN games_globalgameplayer ggp2 ON ggp2.game_id = gg.id AND ggp2.player_id = 2
JOIN cards_deck cd2 ON cd2.id = ggp2.deck_list_id
WHERE gg.match_start BETWEEN TIMESTAMP '%s' AND TIMESTAMP '%s'
GROUP BY gg.id;
"""


def dictfetchall(cursor):
	"Return all rows from a cursor as a dict"
	columns = [col[0] for col in cursor.description]
	return [
		dict(zip(columns, row))
		for row in cursor.fetchall()
		]


def execute_query(connection, query):
	cursor = connection.cursor()
	cursor.execute(query)
	return dictfetchall(cursor)


if __name__ == '__main__':
	job_start_time = time.time()

	parser = argparse.ArgumentParser()
	parser.add_argument('--connection', dest='connection', action='store')
	parser.add_argument('--start', dest='start', action='store')
	parser.add_argument('--end', dest='end', action='store')
	parser.add_argument('--dir', dest='dir', action='store')
	args = parser.parse_args()

	connection = psycopg2.connect(args.connection)

	if args.start and args.end:
		start_str = args.start
		end_str = args.end
	else:
		start_date = datetime.now()
		range = timedelta(minutes=1)
		end_date = start_date + range
		date_format = "%Y-%m-%d %H:%M:%S"
		start_str = start_date.strftime(date_format)
		end_str = end_date.strftime(date_format)

	QUERY = TEMPLATE % (start_str, end_str)

	bucket = "hsreplaynet-replays"

	output_file_name = start_str + "_TO_" + end_str + "_inputs.txt"
	output_file_name = output_file_name.replace(" ", "_")
	output_file_name = output_file_name.replace(":", "-")

	if args.dir:
		output_file_name = os.path.join(args.dir, output_file_name)

	with open(output_file_name, 'w') as output:
		ROW_TEMPLATE = "%s:%s:%s\n"
		for row in execute_query(connection, QUERY):
			# This dict represents the structure the load_redshift.py job expects for the metadata.
			metadata={
				"game_id": row["game_id"],
				"game_type": row["game_type"],
				"scenario_id": row["scenario_id"],
				"ladder_season": row["ladder_season"],
				"brawl_season": row["brawl_season"],
				"players": {
					"1": {
						"deck_id": row["player1_deck_id"],
						"archetype_id": row["player1_archetype_id"],
						"deck_list": row["player1_deck_list"],
						"rank": row["player1_rank"],
						"legend_rank": row["player1_legend_rank"],
						"full_deck_known": row["player1_full_deck_known"]
					},
					"2": {
						"deck_id": row["player2_deck_id"],
						"archetype_id": row["player2_archetype_id"],
						"deck_list": row["player2_deck_list"],
						"rank": row["player2_rank"],
						"legend_rank": row["player2_legend_rank"],
						"full_deck_known": row["player2_full_deck_known"]
					},
				}
			}
			metadata_str = json.dumps(metadata)
			input_str = ROW_TEMPLATE % (bucket, row["replay_xml"], metadata_str)
			output.write(input_str)

	job_end_time = time.time()
	duration_seconds = round(job_end_time - job_start_time, 2)
	summary = "Generated Inputs From: %s To %s (%s Seconds)"
	print(summary % (start_str, end_str, duration_seconds))

