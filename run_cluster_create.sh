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

# This will bootstrap a persistent cluster so that we can iterate more easily during
# development. Once we've got the kinks ironed out, we can bootstrap the cluster on the
# fly.

# Eventually we will need to tune these three values to get ideal splits for our workloads
HADOOP_ARGS="-D fs.s3n.block.size=VALUE1 -D mapred.max.split.size=VALUE2 -D mapred.min.split.size=VALUE2"

# It will produce a cluster id like 'j-2LDN16FBCNO86' which is then provided when kicking
# off an mrjob.
mrjob create-cluster --max-hours-idle 2 --conf-path mrjob.conf --cloud-log-dir s3://mrjob-us-east-1/ --iam-instance-profile arn:aws:iam::746159132190:instance-profile/EMRDefaultRole
