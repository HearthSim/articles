import os
from datetime import datetime, timedelta

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "hsreplaynet.settings")


def main():
	import django; django.setup()
	from hsreplaynet.games.models import GameReplay

	BASE_DIR = os.path.dirname(os.path.abspath(__file__))
	OUT_FILE = os.path.join(BASE_DIR, "jobs/inputs.txt")

	lookback_horizon = datetime.now() - timedelta(hours=1)
	bucket = "hsreplaynet-replays"
	replays = GameReplay.objects.filter(
		global_game__match_start__gte=lookback_horizon
	).all()

	with open(OUT_FILE, "wt") as f:
		for replay in replays:
			f.write("%s:%s\n" % (bucket, str(replay.replay_xml)))


if __name__ == "__main__":
	main()
