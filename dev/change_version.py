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

"""
This script changes versions in every pom.xml and relevant files.

(How to run)
python change_version <reef_home> <reef_version_for_pom.xml>

(Example)
python change_version ~/incubator_reef 0.12.0-incubating-SNAPSHOT
"""


import os
import re
import sys

"""
Get list of path for every file in a directory
"""
def get_filepaths(directory):
    file_paths = []  

    for root, directories, files in os.walk(directory):
        for filename in files:
            filepath = os.path.join(root, filename)
            file_paths.append(filepath)
    return file_paths

"""
Change REEF version to new_version in every pom.xml
"""
def change_pom(file, new_version):
    changed_str = ""

    f = open(file, 'r')

    while True:
        line = f.readline()
        if "<version>" in line:
            break
        changed_str += line

    r = re.compile('<version>(.*?)</version>')
    m = r.search(line)
    old_version = m.group(1)
    changed_str += line.replace(old_version, new_version)

    while True:
        line = f.readline()
        if not line: 
            break
        changed_str += line

    f = open(file, 'w')
    f.write(changed_str)
    f.close()

"""
Change JavaBridgeJarFileName in lang/cs/Org.Apache.REEF.Driver/Constants.cs
"""
def change_constants_cs(file, new_version):
    changed_str = ""

    f = open(file, 'r')
    while True:
        line = f.readline()
        if not line:
            break

        if "JavaBridgeJarFileName" in line:
            r = re.compile('"(.*?)"')
            m = r.search(line)
            old_version = m.group(1)
            new_version = "reef-bridge-java-" + new_version + "-shaded.jar"
            changed_str += line.replace(old_version, new_version)
        else:
            changed_str += line

    f = open(file, 'w')
    f.write(changed_str)
    f.close()

"""
Change the name of shaded.jar in lang/cs/Org.Apache.REEF.Client/run.cmd
"""
def change_shaded_jar_name(file, new_version):
    changed_str =""

    f = open(file, 'r')
    while True:
        line = f.readline()
        if not line:
            break

        if "set SHADED_JAR" in line:
            changed_str += "set SHADED_JAR=.\\reef-bridge-java-" + new_version + "-shaded.jar\n"
        else:
            changed_str += line

    f = open(file, 'w')
    f.write(changed_str)
    f.close()

"""
Change version of every pom.xml, Constants.cs and run.cmd
"""
def change_version(reef_home, new_version):
    for fi in get_filepaths(reef_home):
        if "pom.xml" in fi:
            print fi
            change_pom(fi, new_version)

    change_constants_cs(reef_home + "/lang/cs/Org.Apache.REEF.Driver/Constants.cs", new_version)
    print reef_home + "/lang/cs/Org.Apache.REEF.Driver/Constants.cs"

    change_shaded_jar_name(reef_home + "/lang/cs/Org.Apache.REEF.Client/run.cmd", new_version)
    print reef_home + "/lang/cs/Org.Apache.REEF.Client/run.cmd"


if __name__ == "__main__":
    reef_home = sys.argv[1]
    new_version = sys.argv[2]
    change_version(reef_home, new_version)



