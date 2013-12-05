#!/bin/sh
#
# Copyright (C) 2013 Microsoft Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


# RUNTIME
SELF_JAR=`echo $REEF_HOME/reef-examples/target/reef-examples-*.jar`
REEF_JAR=`echo $REEF_HOME/reef-runtime-yarn/target/reef-runtime-yarn-*-jar-with-dependencies.jar`
LOCAL_JAR=`echo $REEF_HOME/reef-runtime-local/target/reef-runtime-local-*.jar`

# LOCAL_RUNTIME_TMP="-Dcom.microsoft.reef.runtime.local.folder=$REEF_HOME/reef-examples/REEF_RUNTIME_LOCAL/"
LOGGING_CONFIG='-Djava.util.logging.config.class=com.microsoft.reef.util.logging.Config'

CLASSPATH=$YARN_CONF_DIR:$YARN_HOME/share/hadoop/common/lib/*:$YARN_HOME/share/hadoop/common/*:$YARN_HOME/contrib/capacity-scheduler/*.jar:$YARN_HOME/share/hadoop/hdfs:$YARN_HOME/share/hadoop/hdfs/lib/*:$YARN_HOME/share/hadoop/hdfs/*:$YARN_HOME/share/hadoop/yarn/lib/*:$YARN_HOME/share/hadoop/yarn/*:$YARN_HOME/share/hadoop/mapreduce/lib/*:$YARN_HOME/share/hadoop/mapreduce/*:$CLASSPATH

CMD="java -cp $REEF_JAR:$LOCAL_JAR:$SELF_JAR:$CLASSPATH $LOCAL_RUNTIME_TMP $LOGGING_CONFIG $*"
echo $CMD
$CMD # 2> /dev/null
