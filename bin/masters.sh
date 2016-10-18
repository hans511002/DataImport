#!/usr/bin/env bash
#
# Run a shell command on all master hosts.
#
# Environment Variables
#
#   DIMPORT_MASTERS File naming remote hosts.
#     Default is ${HBASE_CONF_DIR}/masters
#   DIMPORT_CONF_DIR  Alternate DIMPORT conf dir. Default is ${DIMPORT_HOME}/conf.
#   DIMPORT_SLAVE_SLEEP Seconds to sleep between spawning remote commands.
#   DIMPORT_SSH_OPTS Options passed to ssh when running remote commands.
#

usage="Usage: $0 [--config <dimport-confdir>] command..."

# if no args specified, show usage
if [ $# -le 0 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin">/dev/null; pwd`

. "$bin"/dimport-config.sh
 
# If the master backup file is specified in the command line,
# then it takes precedence over the definition in
# dimport-env.sh. Save it here.
HOSTLIST=$DIMPORT_MASTERS

if [ "$HOSTLIST" = "" ]; then
  if [ "$DIMPORT_MASTERS" = "" ]; then
    export HOSTLIST="${DIMPORT_CONF_DIR}/masters"
  else
    export HOSTLIST="${DIMPORT_MASTERS}"
  fi
fi


args=${@// /\\ }
args=${args/master-backup/master}

 

if [ -f $HOSTLIST ]; then
  for emaster in `cat "$HOSTLIST"`; do
   ssh $DIMPORT_SSH_OPTS $emaster $"$args  " 2>&1 | sed "s/^/$emaster: /" &
   if [ "$DIMPORT_SLAVE_SLEEP" != "" ]; then
     sleep $DIMPORT_SLAVE_SLEEP
   fi
  done
fi

wait
