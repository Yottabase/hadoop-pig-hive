#! /usr/bin/env bash


DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd $DIR

#rm -v *.log
#rm -Rv ../data/output/hive/*

#start-dfs.sh

echo "STARTING ES1..."

hive -f es1.hql