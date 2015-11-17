#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

/usr/sbin/sshd

/root/sync-hosts.sh

grep hdn /etc/hosts | awk '{print $1}' | sort | uniq > $HADOOP_PREFIX/etc/hadoop/slaves
for host in `cat $HADOOP_PREFIX/etc/hadoop/slaves`
do
    scp /etc/hosts $host:/etc/hosts
    scp $HADOOP_PREFIX/etc/hadoop/slaves $host:$HADOOP_PREFIX/etc/hadoop/slaves
done

hdfs namenode -format

hadoop-daemon.sh --script hdfs start namenode
slaves.sh /usr/hdp/2.3.2.0-2950/hadoop/sbin/hadoop-daemon.sh --script hdfs start datanode

yarn-daemon.sh start resourcemanager
slaves.sh /usr/hdp/2.3.2.0-2950/hadoop-yarn/sbin/yarn-daemon.sh start nodemanager

cd ~ && /bin/bash
