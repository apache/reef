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

FROM reefrt/apache2.7
MAINTAINER Apache REEF <dev@reef.apache.org>

RUN ln -s /usr/lib/jvm/java-7-oracle/jre/lib/amd64/server/libjvm.so /usr/bin/libjvm.so

# Apache Mesos 0.25.0
RUN \
  apt-key adv --keyserver keyserver.ubuntu.com --recv E56151BF && \
  echo "deb http://repos.mesosphere.io/ubuntu precise main" > /etc/apt/sources.list.d/mesosphere.list && \
  apt-get -y update && \
  apt-get install -y mesos && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*
ENV HADOOP_HOME=$HADOOP_PREFIX
RUN echo 'PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/local/hadoop/bin"' >> /etc/environment
COPY init-nn.sh /root/

EXPOSE 22 5050
