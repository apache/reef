#!/bin/sh
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

# EXAMPLE USAGE 
# . ./bin/runazuretests.sh 
# "./lang/java/reef-examples/target/reef-examples-0.17.0-SNAPSHOT-shaded.jar;
# ./lang/java/reef-tests/target/reef-tests-0.17.0-SNAPSHOT-test-jar-with-dependencies.jar" 
# org.apache.reef.tests.examples.TestHelloREEF


# RUNTIME

if [ $# -ne 2 ];
then 
    echo "Only 2 arguments are accepted - CLASSPATH and TESTCLASS"
    exit 1;
fi

[ -z "$VARIABLE" ] && VARIABLE="REEF_TEST_AZBATCH"

if [ $VARIABLE != "true" ]
then
    echo "Trying to set REEF_TEST_AZBATCH environment variable."
    echo "Please run as \". runazuretests.sh\" or set it from your environment."
    export REEF_TEST_AZBATCH=true
fi

CLASSPATH=$1
TESTCLASS=$2

CMD="java -cp ${CLASSPATH} org.junit.runner.JUnitCore ${TESTCLASS}"

echo -e "\n\nRunning Azure Batch Tests...\n\n"
echo $CMD
$CMD
