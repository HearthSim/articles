#!/bin/bash


if [ ! -z "$WORKSPACE" ]; then
	# Jenkins hack...
	BASEDIR="$WORKSPACE/scripts"
elif [ "$(uname)" = "Darwin" ]; then
	BASEDIR="$(dirname $(stat -f ${BASH_SOURCE[0]}))"
else
	BASEDIR="$(readlink -f $(dirname $0))"
fi

DATADIR="$BASEDIR/../build"
GITDIR="$DATADIR/hsreplay-test-data"
TESTDATA_URL="https://github.com/HearthSim/hsreplay-test-data.git"

set -e

mkdir -p "$DATADIR"

if [[ -d $GITDIR ]]; then
	echo "Updating $GITDIR"
	git -C "$GITDIR" fetch -q --all
	git -C "$GITDIR" reset -q --hard origin/master
else
	git clone "$TESTDATA_URL" "$GITDIR"
fi

