#! /usr/bin/env bash


DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd $DIR

rm -Rv ../data/output/hive/*

echo "STARTING ES1..."
hive -f es1.hql -hiveconf INPUT="../data/generator/sample/esempio.txt" -hiveconf OUTPUT="../data/output/hivea/"

echo "STARTING ES2..."
hive -f es2.hql -hiveconf INPUT="../data/generator/sample/esempio.txt" -hiveconf OUTPUT="../data/output/hiveb/"

echo "STARTING ES3..."
hive -f es3.hql -hiveconf INPUT="../data/generator/sample/esempio.txt" -hiveconf OUTPUT="../data/output/hivec/"

echo "STARTING OPT1..."
hive -f opt1.hql -hiveconf INPUT="../data/generator/sample/esempio.txt" -hiveconf OUTPUT="../data/output/hived/"