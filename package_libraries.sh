#!/bin/bash

if [[ -a ./replay_analysis_lib.tar.gz ]]; then
    rm ./replay_analysis_lib.tar.gz
fi

tar -C lib -f replay_analysis_lib.tar.gz -z -c .