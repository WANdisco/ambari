#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cygwin=false
case "`uname`" in
   CYGWIN*) cygwin=true;;
esac

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

## wdd workaround
bin="/usr/wdd/current/hive-client/bin"

. "$bin"/hive-config.sh
HIVE_HOME="/usr/wdd/current/hive-client"


TMP_USER_DIR="/tmp/${USER}"
STDERR="${TMP_USER_DIR}/stderr"
SERVICE=""
HELP=""
while [ $# -gt 0 ]; do
  case "$1" in
    --version)
      shift
      SERVICE=version
      ;;
    --service)
      shift
      SERVICE=$1
      shift
      ;;
    --rcfilecat)
      SERVICE=rcfilecat
      shift
      ;;
    --orcfiledump)
      SERVICE=orcfiledump
      shift
      ;;
    --llapdump)
      SERVICE=llapdump
      shift
      ;;
    --help)
      HELP=_help
      shift
      ;;
    --debug*)
      DEBUG=$1
      shift
      ;;
    *)
      break
      ;;
  esac
done

if [ "$SERVICE" = "" ] ; then
  if [ "$HELP" = "_help" ] ; then
    SERVICE="help"
  else
    SERVICE="cli"
  fi
fi

if [ -f "${HIVE_CONF_DIR}/hive-env.sh" ]; then
  . "${HIVE_CONF_DIR}/hive-env.sh"
fi

if [[ -z "$SPARK_HOME" ]]
then
  bin=`dirname "$0"`
  # many hadoop installs are in dir/{spark,hive,hadoop,..}
  if test -e $bin/../../spark; then
    sparkHome=$(readlink -f $bin/../../spark)
    if [[ -d $sparkHome ]]
    then
      export SPARK_HOME=$sparkHome
    fi
  fi
fi

CLASSPATH="${HIVE_CONF_DIR}"

HIVE_LIB=${HIVE_HOME}/lib

# needed for execution
if [ ! -f ${HIVE_LIB}/hive-exec-*.jar ]; then
  echo "Missing Hive Execution Jar: ${HIVE_LIB}/hive-exec-*.jar"
  exit 1;
fi

if [ ! -f ${HIVE_LIB}/hive-metastore-*.jar ]; then
  echo "Missing Hive MetaStore Jar"
  exit 2;
fi

# cli specific code
if [ ! -f ${HIVE_LIB}/hive-cli-*.jar ]; then
  echo "Missing Hive CLI Jar"
  exit 3;
fi

for f in ${HIVE_LIB}/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done

# add the auxillary jars such as serdes
if [ -d "${HIVE_AUX_JARS_PATH}" ]; then
  hive_aux_jars_abspath=`cd ${HIVE_AUX_JARS_PATH} && pwd`
  for f in $hive_aux_jars_abspath/*.jar; do
    if [[ ! -f $f ]]; then
        continue;
    fi
    if $cygwin; then
	f=`cygpath -w "$f"`
    fi
    AUX_CLASSPATH=${AUX_CLASSPATH}:$f
    if [ "${AUX_PARAM}" == "" ]; then
        AUX_PARAM=file://$f
    else
        AUX_PARAM=${AUX_PARAM},file://$f;
    fi
  done
elif [ "${HIVE_AUX_JARS_PATH}" != "" ]; then
  HIVE_AUX_JARS_PATH=`echo $HIVE_AUX_JARS_PATH | sed 's/,/:/g'`
  if $cygwin; then
      HIVE_AUX_JARS_PATH=`cygpath -p -w "$HIVE_AUX_JARS_PATH"`
      HIVE_AUX_JARS_PATH=`echo $HIVE_AUX_JARS_PATH | sed 's/;/,/g'`
  fi
  AUX_CLASSPATH=${AUX_CLASSPATH}:${HIVE_AUX_JARS_PATH}
  AUX_PARAM="file://$(echo ${HIVE_AUX_JARS_PATH} | sed 's/:/,file:\/\//g')"
fi

# adding jars from auxlib directory
for f in ${HIVE_HOME}/auxlib/*.jar; do
  if [[ ! -f $f ]]; then
      continue;
  fi
  if $cygwin; then
      f=`cygpath -w "$f"`
  fi
  AUX_CLASSPATH=${AUX_CLASSPATH}:$f
  if [ "${AUX_PARAM}" == "" ]; then
    AUX_PARAM=file://$f
  else
    AUX_PARAM=${AUX_PARAM},file://$f;
  fi
done
if $cygwin; then
    CLASSPATH=`cygpath -p -w "$CLASSPATH"`
    CLASSPATH=${CLASSPATH};${AUX_CLASSPATH}
else
    CLASSPATH=${CLASSPATH}:${AUX_CLASSPATH}
fi

# supress the HADOOP_HOME warnings in 1.x.x
export HADOOP_HOME_WARN_SUPPRESS=true

# to make sure log4j2.x and jline jars are loaded ahead of the jars pulled by hadoop
export HADOOP_USER_CLASSPATH_FIRST=true

# pass classpath to hadoop
if [ "$HADOOP_CLASSPATH" != "" ]; then
  export HADOOP_CLASSPATH="${CLASSPATH}:${HADOOP_CLASSPATH}"
else
  export HADOOP_CLASSPATH="$CLASSPATH"
fi

# also pass hive classpath to hadoop
if [ "$HIVE_CLASSPATH" != "" ]; then
  export HADOOP_CLASSPATH="${HADOOP_CLASSPATH}:${HIVE_CLASSPATH}";
fi

# check for hadoop in the path
HADOOP_IN_PATH=`which hadoop 2>/dev/null`
if [ -f ${HADOOP_IN_PATH} ]; then
  HADOOP_DIR=`dirname "$HADOOP_IN_PATH"`/..
fi
# HADOOP_HOME env variable overrides hadoop in the path
HADOOP_HOME=${HADOOP_HOME:-${HADOOP_PREFIX:-$HADOOP_DIR}}
if [ "$HADOOP_HOME" == "" ]; then
  echo "Cannot find hadoop installation: \$HADOOP_HOME or \$HADOOP_PREFIX must be set or hadoop must be in the path";
  exit 4;
fi

HADOOP=$HADOOP_HOME/bin/hadoop
if [ ! -f ${HADOOP} ]; then
  echo "Cannot find hadoop installation: \$HADOOP_HOME or \$HADOOP_PREFIX must be set or hadoop must be in the path";
  exit 4;
fi

if [ ! -d ${TMP_USER_DIR} ]; then
  mkdir -p ${TMP_USER_DIR} 2> /dev/null
  if [ $? -ne 0 ]; then
    STDERR="/dev/tty"
  fi
fi

if [ "${STDERR}" != "/dev/null" ] && [ ! -f ${STDERR} ]; then
  touch ${STDERR} 2> /dev/null
  if [ $? -ne 0 ]; then
    STDERR="/dev/tty"
  fi
fi

# Make sure we're using a compatible version of Hadoop
if [ "x$HADOOP_VERSION" == "x" ]; then
    HADOOP_VERSION=$($HADOOP version 2>> ${STDERR} | awk -F"\t" '/Hadoop/ {print $0}' | cut -d' ' -f 2);
fi

# Save the regex to a var to workaround quoting incompatabilities
# between Bash 3.1 and 3.2
hadoop_version_re="^([[:digit:]]+)\.([[:digit:]]+)(\.([[:digit:]]+))?.*$"

if [[ "$HADOOP_VERSION" =~ $hadoop_version_re ]]; then
    hadoop_major_ver=${BASH_REMATCH[1]}
    hadoop_minor_ver=${BASH_REMATCH[2]}
    hadoop_patch_ver=${BASH_REMATCH[4]}
else
    echo "Unable to determine Hadoop version information."
    echo "'hadoop version' returned:"
    echo `$HADOOP version`
    exit 5
fi

if [ "$hadoop_major_ver" -lt "1" -a  "$hadoop_minor_ver$hadoop_patch_ver" -lt "201" ]; then
    echo "Hive requires Hadoop 0.20.x (x >= 1)."
    echo "'hadoop version' returned:"
    echo `$HADOOP version`
    exit 6
fi

# HBase detection. Need bin/hbase and a conf dir for building classpath entries.
# Start with BigTop defaults for HBASE_HOME and HBASE_CONF_DIR.
HBASE_HOME=${HBASE_HOME:-"/usr/lib/hbase"}
HBASE_CONF_DIR=${HBASE_CONF_DIR:-"/etc/hbase/conf"}
if [[ ! -d $HBASE_CONF_DIR ]] ; then
  # not explicitly set, nor in BigTop location. Try looking in HBASE_HOME.
  HBASE_CONF_DIR="$HBASE_HOME/conf"
fi

# perhaps we've located the HBase config. if so, include it on classpath.
if [[ -d $HBASE_CONF_DIR ]] ; then
  export HADOOP_CLASSPATH="${HADOOP_CLASSPATH}:${HBASE_CONF_DIR}"
fi

# look for the hbase script. First check HBASE_HOME and then ask PATH.
if [[ -e $HBASE_HOME/bin/hbase ]] ; then
  HBASE_BIN="$HBASE_HOME/bin/hbase"
fi
HBASE_BIN=${HBASE_BIN:-"$(which hbase)"}

# perhaps we've located HBase. If so, include its details on the classpath
if [[ -n $HBASE_BIN ]] ; then
  # exclude ZK, PB, and Guava (See HIVE-2055)
  # depends on HBASE-8438 (hbase-0.94.14+, hbase-0.96.1+) for `hbase mapredcp` command
  for x in $($HBASE_BIN mapredcp 2>> ${STDERR} | tr ':' '\n') ; do
    if [[ $x == *zookeeper* || $x == *protobuf-java* || $x == *guava* ]] ; then
      continue
    fi
    # TODO: should these should be added to AUX_PARAM as well?
    export HADOOP_CLASSPATH="${HADOOP_CLASSPATH}:${x}"
  done
fi

if [ "${AUX_PARAM}" != "" ]; then
  if [[ "$SERVICE" != beeline ]]; then
    HIVE_OPTS="$HIVE_OPTS --hiveconf hive.aux.jars.path=${AUX_PARAM}"
  fi
  AUX_JARS_CMD_LINE="-libjars ${AUX_PARAM}"
fi

SERVICE_LIST=""

for i in "$HIVE_HOME"/bin/ext/*.sh ; do
  . $i
done

for i in "$HIVE_HOME"/bin/ext/util/*.sh ; do
  . $i
done

if [ "$DEBUG" ]; then
  if [ "$HELP" ]; then
    debug_help
    exit 0
  else
    get_debug_params "$DEBUG"
    export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS $HIVE_MAIN_CLIENT_DEBUG_OPTS"
  fi
fi

TORUN=""
for j in $SERVICE_LIST ; do
  if [ "$j" = "$SERVICE" ] ; then
    TORUN=${j}$HELP
  fi
done

# to initialize logging for all services
export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS -Dlog4j.configurationFile=hive-log4j2.properties "

if [ -f "${HIVE_CONF_DIR}/parquet-logging.properties" ]; then
  export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS -Djava.util.logging.config.file=${HIVE_CONF_DIR}/parquet-logging.properties "
else
  export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS -Dlog4j.configurationFile=hive-log4j2.properties -Djava.util.logging.config.file=$bin/../conf/parquet-logging.properties "
fi

if [ "$TORUN" = "" ] ; then
  echo "Service $SERVICE not found"
  echo "Available Services: $SERVICE_LIST"
  exit 7
else
  $TORUN "$@"
fi