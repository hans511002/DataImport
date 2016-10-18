#!/usr/bin/env bash
#
#
# Run a dimport command on all master hosts.
# Modelled after $DIMPORT_HOME/bin/dimport-daemons.sh

usage="Usage: dimport-daemons.sh [--config <dimport-confdir>] \
 [--hosts serversfile] [start|stop] command args..."

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin">/dev/null; pwd`

. $bin/dimport-config.sh

remote_cmd="cd ${DIMPORT_HOME}; $bin/dimport-daemon.sh --config ${DIMPORT_CONF_DIR} $@"
args="--hosts ${DIMPORT_MASTERS} --config ${DIMPORT_CONF_DIR} $remote_cmd"

command=$2

case $command in
  (master|server)
    exec "$bin/masters.sh" $args
    ;;
  (*)
   # exec "$bin/servers.sh" $args
    ;;
esac

