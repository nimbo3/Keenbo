#!/usr/bin/env bash
export BASEDIR=$(dirname "$0")
cd $BASEDIR
mysql -u root -p < init.sql
