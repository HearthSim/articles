import csv
import sys
import json

CLASSES = [
	"DRUID", "HUNTER", "MAGE",
	"PALADIN", "PRIEST", "ROGUE",
	"SHAMAN", "WARLOCK", "WARRIOR"
]

file = sys.argv[1]
key = sys.argv[2] if len(sys.argv) > 2 else "key"
output = []


def generate(row):
	data = {}
	data["key"] = row[key]
	for c in CLASSES:
		data[c.lower()] = int(row[c])
	return data

with open(file) as f:
	reader = csv.DictReader(f)
	for row in reader:
		output.append(generate(row))

with open("output.json", "w", encoding="utf-8") as f:
	f.write(json.dumps(output))
