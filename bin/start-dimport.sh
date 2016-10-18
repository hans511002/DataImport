#!/usr/bin/env bash
#

# Modelled after $DIMPORT_HOME/bin/start-dimport.sh.

# Run this on master node.
usage="Usage: start-dimport.sh"

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin">/dev/null; pwd`

. "$bin"/dimport-config.sh

# start DIMPORT daemons
errCode=$?
if [ $errCode -ne 0 ]
then
  exit $errCode
fi


if [ "$1" = "autorestart" ]
then
  commandToRun="autorestart"
else
  commandToRun="start"
fi


"$bin"/dimport-daemons.sh --config "${DIMPORT_CONF_DIR}" --hosts "${DIMPORT_MASTERS}" $commandToRun master


