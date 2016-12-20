#!/usr/bin/env python

from setuptools import setup, find_packages

CLASSIFIERS = [
	"Intended Audience :: Developers",
	"Programming Language :: Python",
	"Programming Language :: Python :: 2",
	"Programming Language :: Python :: 2.7",
	"Programming Language :: Python :: 3",
	"Programming Language :: Python :: 3.4",
	"Programming Language :: Python :: 3.5",
	"Topic :: Games/Entertainment :: Simulation",
]

setup(
	name="replay-analysis",
	version="0.0.1",
	packages=find_packages(),
	author="Andrew Wilson",
	author_email="andrew@hearthsim.net",
	description="Mapreduce and Redshift libraries for analytics on HSReplay.xml files in Python",
	classifiers=CLASSIFIERS,
	download_url="https://github.com/HearthSim/replay-analysis/tarball/master",
	license="All Rights Reserved",
	url="https://github.com/HearthSim/replay-analysis",
	zip_safe=True,
)