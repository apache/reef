#!/usr/bin/env bash
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

ORG=reefrt
for DIR in `ls -d ubuntu12.04*`
do
    IMAGE=$DIR
    TAG=${DIR##*-}
    echo $DIR $ORG/$TAG
    docker build -t $ORG/$IMAGE $DIR
    docker tag -f $ORG/$IMAGE $ORG/$TAG
done
docker tag -f $ORG/ubuntu12.04-jdk7-hdp2.1.15 $ORG/hdi3.1
docker tag -f $ORG/ubuntu12.04-jdk7-hdp2.2.7 $ORG/hdi3.2
docker tag -f $ORG/ubuntu12.04-jdk7-hdp2.2.8 $ORG/hdp2.2
docker tag -f $ORG/ubuntu12.04-jdk7-hdp2.3.2 $ORG/hdp2.3
