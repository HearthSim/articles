#!/usr/bin/env bash

if [[ ! -z $WORKSPACE ]]; then
	# Jenkins hack...
	BASEDIR="$WORKSPACE/scripts"
elif [[ "$(uname)" = "Darwin" ]]; then
	BASEDIR="$(dirname ${BASH_SOURCE[0]})"
else
	BASEDIR="$(readlink -f $(dirname $0))"
fi

export ROOTDIR="$BASEDIR/../.."
cd "$ROOTDIR"

tar -C "$ROOTDIR" -f hsreplaynet.tar.gz -z -c .

export PYTHONPATH="$ROOTDIR"

SCRIPT="$ROOTDIR/scripts/jobs/$1"
INPUTS="$ROOTDIR/scripts/jobs/inputs.txt"
CONF="$ROOTDIR/scripts/jobs/mrjob.conf"
CLUSTER_ID="j-1YKKRHJGLSWC9"

# We can use output dir to control where the final file produced is saved on S3
# --output-dir "$OUTPUT_DIR"
OUTPUT_DIR="s3://mrjob-us-east-1/"

python "$SCRIPT" -r emr "$INPUTS" --conf-path "$CONF" --cluster-id "$CLUSTER_ID"

# Follow these steps to start a persistent cluster so we can iterate faster
# Make sure SSH is working so we can fetch the most current logs
# http://mrjob.readthedocs.io/en/latest/guides/emr-troubleshooting.html
