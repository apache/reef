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

if [ $# -gt 0 ]
then
  echo "Usage: ./publish.sh"
  echo "       (with no arguments)"
  exit 1
fi

echo
echo "Shell script for uploading REEF website contents to Apache SVN!"

# When adding new javadocs (e.g. a new release), execute
# 'mvn javadoc:aggregate' in \$REEF_HOME and put the resulting
# \$REEF_HOME/target/apidocs folder in the following directory.
echo "Newly updated javadocs (if there is any) should be in the directory:"
echo "  \$REEF_HOME/website/target/staging/site/apidocs/\${reef.version}"
echo "Please run this script in \$REEF_HOME/website."
echo

read -p "Apache Username: " username
read -s -p "Apache Password: " password
echo
read -p "Commit Message (empty string for default msg): " comment

if [ -z "$comment" ]
then
  mvn site:site scm-publish:publish-scm -Dusername="$username" -Dpassword="$password"
else
  mvn site:site scm-publish:publish-scm -Dusername="$username" -Dpassword="$password" -Dscmpublish.checkinComment="$comment"
fi

