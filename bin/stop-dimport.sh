#!/usr/bin/env bash
#
# Modelled after $DIMPORT_HOME/bin/stop-dimport.sh.

# Stop  dimport daemons.  Run this on master node.

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin">/dev/null; pwd`

. "$bin"/dimport-config.sh
. "$bin"/dimport-common.sh

# variables needed for stop command
if [ "$DIMPORT_LOG_DIR" = "" ]; then
  export DIMPORT_LOG_DIR="$DIMPORT_HOME/logs"
fi
mkdir -p "$DIMPORT_LOG_DIR"

if [ "$DIMPORT_IDENT_STRING" = "" ]; then
  export DIMPORT_IDENT_STRING="$USER"
fi

export DIMPORT_LOG_PREFIX=hbase-$DIMPORT_IDENT_STRING-master-$HOSTNAME
export DIMPORT_LOGFILE=$DIMPORT_LOG_PREFIX.log
logout=$DIMPORT_LOG_DIR/$DIMPORT_LOG_PREFIX.out
loglog="${DIMPORT_LOG_DIR}/${DIMPORT_LOGFILE}"
pid=${DIMPORT_PID_DIR:-/tmp}/dimport-$DIMPORT_IDENT_STRING-master.pid

echo  "stoping dimport ...  "  
"$bin"/dimport-daemons.sh --config "${DIMPORT_CONF_DIR}" --hosts "${DIMPORT_MASTERS}" stop master

 