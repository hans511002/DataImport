#!/usr/bin/env bash

# Runs a dimport command as a daemon.
#
# Environment Variables
#
#   DIMPORT_CONF_DIR   Alternate dimport conf dir. Default is ${DIMPORT_HOME}/conf.
#   DIMPORT_LOG_DIR    Where log files are stored.  PWD by default.
#   DIMPORT_PID_DIR    The pid files are stored. /tmp by default.
#   DIMPORT_IDENT_STRING   A string representing this instance of hadoop. $USER by default
#   DIMPORT_NICENESS The scheduling priority for daemons. Defaults to 0.
#   DIMPORT_STOP_TIMEOUT  Time, in seconds, after which we kill -9 the server if it has not stopped.
#                        Default 1200 seconds.
#
 
usage="Usage: dimport-daemon.sh [--config <conf-dir>]\
 (start|stop|restart|autorestart) <dimport-command> \
 <args...>"

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin">/dev/null; pwd`

. "$bin"/dimport-config.sh
. "$bin"/dimport-common.sh

if [ -f "$DIMPORT_HOME/conf/dimport-env.sh" ]; then
  . "$DIMPORT_HOME/conf/dimport-env.sh"
fi

# get arguments
startStop=$1
shift

command=$1
shift

dimport_rotate_log ()
{
    log=$1;
    num=5;
    if [ -n "$2" ]; then
    num=$2
    fi
    if [ -f "$log" ]; then # rotate logs
    while [ $num -gt 1 ]; do
        prev=`expr $num - 1`
        [ -f "$log.$prev" ] && mv -f "$log.$prev" "$log.$num"
        num=$prev
    done
    mv -f "$log" "$log.$num";
    fi
}

cleanZNode() {
  if [ -f $DIMPORT_ZNODE_FILE ]; then
    if [ "$command" = "master" ]; then
      $bin/dimport master clear > /dev/null 2>&1
    else
        $bin/dimport clean --cleanZk  > /dev/null 2>&1
    fi
    rm $DIMPORT_ZNODE_FILE
  fi
}

check_before_start(){
    #ckeck if the process is not running
    mkdir -p "$DIMPORT_PID_DIR"
    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo $command running as process `cat $pid`.  Stop it first.
        exit 1
      fi
    fi
}

wait_until_done ()
{
    p=$1
    cnt=${DIMPORT_SLAVE_TIMEOUT:-300}
    origcnt=$cnt
    while kill -0 $p > /dev/null 2>&1; do
      if [ $cnt -gt 1 ]; then
        cnt=`expr $cnt - 1`
        sleep 1
      else
        echo "Process did not complete after $origcnt seconds, killing."
        kill -9 $p
        exit 1
      fi
    done
    return 0
}

# get log directory
if [ "$DIMPORT_LOG_DIR" = "" ]; then
  export DIMPORT_LOG_DIR="$DIMPORT_HOME/logs"
fi
mkdir -p "$DIMPORT_LOG_DIR"

if [ "$DIMPORT_PID_DIR" = "" ]; then
  DIMPORT_PID_DIR=$DIMPORT_LOG_DIR
fi

if [ "$DIMPORT_IDENT_STRING" = "" ]; then
  export DIMPORT_IDENT_STRING="$USER"
fi

# Some variables
# Work out java location so can print version into log.
if [ "$JAVA_HOME" != "" ]; then
  #echo "run java in $JAVA_HOME"
  JAVA_HOME=$JAVA_HOME
fi
if [ "$JAVA_HOME" = "" ]; then
  echo "Error: JAVA_HOME is not set."
  exit 1
fi

JAVA=$JAVA_HOME/bin/java
export DIMPORT_LOG_PREFIX=dimport-$DIMPORT_IDENT_STRING-$command-$HOSTNAME
export DIMPORT_LOGFILE=$DIMPORT_LOG_PREFIX.log
export DIMPORT_ROOT_LOGGER=${DIMPORT_ROOT_LOGGER:-"INFO,RFA"}
export DIMPORT_SECURITY_LOGGER=${DIMPORT_SECURITY_LOGGER:-"INFO,RFAS"}
logout=$DIMPORT_LOG_DIR/$DIMPORT_LOG_PREFIX.out
loggc=$DIMPORT_LOG_DIR/$DIMPORT_LOG_PREFIX.gc
loglog="${DIMPORT_LOG_DIR}/${DIMPORT_LOGFILE}"
pid=$DIMPORT_PID_DIR/dimport-$DIMPORT_IDENT_STRING-$command.pid
export DIMPORT_ZNODE_FILE=$DIMPORT_PID_DIR/dimport-$DIMPORT_IDENT_STRING-$command.znode
export DIMPORT_START_FILE=$DIMPORT_PID_DIR/dimport-$DIMPORT_IDENT_STRING-$command.autorestart

if [ -n "$SERVER_GC_OPTS" ]; then
  export SERVER_GC_OPTS=${SERVER_GC_OPTS/"-Xloggc:<FILE-PATH>"/"-Xloggc:${loggc}"}
fi

# Set default scheduling priority
if [ "$DIMPORT_NICENESS" = "" ]; then
    export DIMPORT_NICENESS=0
fi

thiscmd=$0
args=$@

case $startStop in

(start)
    check_before_start
    dimport_rotate_log $logout
    dimport_rotate_log $loggc
    
     if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo $command running as process `cat $pid`.  Stop it first.
        exit 1
      fi
    fi
  
    echo starting $command, logging to $logout
    cd "$STORM_HOME"
    nohup nice -n $DIMPORT_NICENESS $DIMPORT_HOME/bin/dimport $command start "$@" < /dev/null > ${logout} 2>&1 &
    #nohup nice -n $DIMPORT_NICENESS $DIMPORT_HOME/bin/dimport $command start "$@" > "${logout}" 2>&1 < /dev/null &
    echo $! > $pid
    sleep 1; head "${logout}"
  ;;

 
(stop)
    rm -f "$DIMPORT_START_FILE"
    if [ -f $pid ]; then
      pidToKill=`cat $pid`
      # kill -0 == see if the PID exists
      if kill -0 $pidToKill > /dev/null 2>&1; then
        echo -n stopping $command
        echo "`date` Terminating $command" >> $loglog
        kill $pidToKill > /dev/null 2>&1
        waitForProcessEnd $pidToKill $command
        rm $pid
      else
        retval=$?
        echo no $command to stop because kill -0 of pid $pidToKill failed with status $retval
      fi
    else
      echo no $command to stop because no pid file $pid
    fi
  ;;

(restart)
    # stop the command
    $thiscmd --config "${DIMPORT_CONF_DIR}" stop $command $args &
    wait_until_done $!
    # wait a user-specified sleep period
    sp=${DIMPORT_RESTART_SLEEP:-3}
    if [ $sp -gt 0 ]; then
      sleep $sp
    fi
    # start the command
    $thiscmd --config "${DIMPORT_CONF_DIR}" start $command $args &
    wait_until_done $!
  ;;

(*)
  echo $usage
  exit 1
  ;;
esac
