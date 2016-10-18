#!/usr/bin/env bash

usage="Usage: dimport-cleanup.sh (--cleanZk)"

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

# This will set DIMPORT_HOME, etc.
. "$bin"/dimport-config.sh

case $1 in
  --cleanZk)
    matches="yes" ;;
  *) ;;
esac
if [ $# -ne 1 -o "$matches" = "" ]; then
  echo $usage
  exit 1;
fi

format_option=$1;

$bin/dimport clean --cleanZk  > /dev/null 2>&1
