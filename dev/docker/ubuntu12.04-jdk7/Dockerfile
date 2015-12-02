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

FROM ubuntu:12.04
MAINTAINER Apache REEF <dev@reef.apache.org>

RUN \
  apt-get update && \
  apt-get install -y openssh-server python-software-properties && \
  apt-get clean

RUN \
  mkdir /var/run/sshd && \
  ssh-keygen -q -N "" -t rsa -f /root/.ssh/id_rsa && \
  cp /root/.ssh/id_rsa.pub /root/.ssh/authorized_keys && \
  sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd && \
  echo 'StrictHostKeyChecking no' > /root/.ssh/config
COPY start.sh /root/

# java
RUN \
  echo oracle-java7-installer shared/accepted-oracle-license-v1-1 select true | debconf-set-selections && \
  add-apt-repository -y ppa:webupd8team/java && \
  apt-get update && \
  apt-get install -y oracle-java7-installer && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/* && \
  rm -rf /var/cache/oracle-jdk7-installer

ENV JAVA_HOME /usr/lib/jvm/java-7-oracle
ENV PATH $PATH:$JAVA_HOME/bin
RUN echo 'JAVA_HOME="/usr/lib/jvm/java-7-oracle"' >> /etc/environment
