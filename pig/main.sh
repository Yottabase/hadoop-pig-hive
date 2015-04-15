#! /usr/bin/env bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd $DIR

rm -v *.log
rm -Rv ../data/output/pig/*

echo "STARTING ES1..."
pig -x local es1.pig

echo "STARTING ES2..."
pig -x local es2.pig

echo "STARTING ES3..."
pig -x local es3.pig

echo "STARTING OPT1..."
pig -x local opt1.pig

echo "STARTING OPT2..."
pig -x local opt2.pig