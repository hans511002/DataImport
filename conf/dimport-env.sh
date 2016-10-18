#!/usr/bin/env bash

# Set environment variables here.

# This script sets variables multiple times over the course of starting an hbase process,
# so try to keep things idempotent unless you want to take an even deeper look
# into the startup scripts (bin/hbase, etc.)

# The java implementation to use.  Java 1.6 required.
# export JAVA_HOME=/usr/java/jdk1.6.0/
. ~/.bash_profile

# Extra Java CLASSPATH elements.  Optional. 
 
export DIMPORT_CLASSPATH=$DIMPORT_HOME/classes:$DIMPORT_HOME/conf 
 
for f in  $DIMPORT_HOME/dimport-*.jar $DIMPORT_HOME/lib/*.jar ; do 
    DIMPORT_CLASSPATH=${DIMPORT_CLASSPATH}:$f; 
done

DEBUG="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=8001"

export DIMPORT_HEAPSIZE=1000

if [ "$DIMPORT_PID_DIR" = "" ]; then
export DIMPORT_PID_DIR=$DIMPORT_HOME/logs
fi
if [ "$DIMPORT_LOG_DIR" = "" ]; then
export DIMPORT_LOG_DIR=$DIMPORT_HOME/logs
fi
 
