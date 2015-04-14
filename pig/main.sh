#! /usr/bin/env bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd $DIR


# pig -x local es1.pig

pig -x local es2.pig
