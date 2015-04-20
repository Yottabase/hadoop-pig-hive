#! /usr/bin/env bash


DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd $DIR

rm -Rv ../data/output/hive/*

echo "STARTING ES1..."
hive -f es1.hql

echo "STARTING ES3..."
hive -f es3.hql

echo "STARTING OPT1..."
hive -f opt1.hql