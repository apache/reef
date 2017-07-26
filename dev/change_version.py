#!/usr/bin/env python
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
python change_version <reef_home> <reef_version_for_pom.xml> -s <true or false>  (optional) -p

-s option changes value of 'IsSnapshot' in lang/cs/build.props.
If you use the option "-s false", bulid.props changes as,
 <IsSnapshot>false</IsSnapshot>
 <SnapshotNumber>00</SnapshotNumber>

If you use "-s true", then the value of 'IsSnapshot' is changed to true.

If you use "-p", then only the "pom.xml" files are changed.

You can also see how to run the script with "python change_version.py -h"

(Example)
python change_version ~/reef 0.14.0 -s true
python change_version ~/reef 0.14.0 -s false
python change_version ~/reef 0.14.0 -p -s true
"""


import os
import re
import sys
import argparse

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
    ready_to_change = False

    f = open(file, 'r')

    while True:
        line = f.readline()
        if not line:
            break
        if "<groupId>org.apache.reef</groupId>" in line:
            ready_to_change = True
        if "<version>" in line and ready_to_change:
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
    f.close()

    f = open(file, 'w')
    f.write(changed_str)
    f.close()

"""
Change JavaBridgeJarFileName in lang/cs/Org.Apache.REEF.Driver/DriverConfigGenerator.cs
"""
def change_constants_cs(file, new_version):
    changed_str = ""

    f = open(file, 'r')
    while True:
        line = f.readline()
        if not line:
            break

        if "JavaBridgeJarFileName =" in line:
            r = re.compile('"(.*?)"')
            m = r.search(line)
            old_version = m.group(1)
            new_version = "reef-bridge-java-" + new_version + "-shaded.jar"
            changed_str += line.replace(old_version, new_version)
        else:
            changed_str += line
    f.close()

    f = open(file, 'w')
    f.write(changed_str)
    f.close()

"""
Change version in SharedAssemblyInfo.cs and AssemblyInfo.cpp
"""
def change_assembly_info_cs(file, new_version):
    changed_str = ""
    new_version = new_version.split("-")[0] + ".0"

    f = open(file, 'r')
    r = re.compile('"(.*?)"')

    while True:
        line = f.readline()
        if not line:
            break
        if ("[assembly: AssemblyVersion(" in line and "*" not in line) or ("[assembly: AssemblyFileVersion(" in line) \
            or ("[assembly:AssemblyVersionAttribute(" in line and "*" not in line) \
            or ("[assembly:AssemblyFileVersion(" in line):
            m = r.search(line)
            old_version = m.group(1)
            changed_str += line.replace(old_version, new_version)
        else:
            changed_str += line
    f.close()

    f = open(file, 'w')
    f.write(changed_str)
    f.close()

"""
Change Version in lang/cs/build.DotNet.props
"""
def change_dotnet_props_cs(file, new_version):
    changed_str = ""
    new_version = new_version
    if "SNAPSHOT" in new_version:
      new_version = new_version.split("-")[0]

    f = open(file, 'r')
    r = re.compile('<Version>(.*?)</Version>')

    while True:
        line = f.readline()
        if not line:
            break
        if "<Version>" in line and "</Version>" in line:
            m = r.search(line)
            old_version = m.group(1)
            changed_str += line.replace(old_version, new_version)
        else:
            changed_str += line
    f.close()

    f = open(file, 'w')
    f.write(changed_str)
    f.close()

"""
Change ReefOnSpark.scala in lang/scala/reef-examples-scala/.../hellospark/ReefOnSpark.scala
"""
def change_reef_on_spark_scala(file, new_version):
    changed_str = ""

    f = open(file, 'r')

    while True:
        line = f.readline()
        if not line:
            break
        if "reef-examples-spark" in line and "-shaded.jar":
            r = re.compile("//.*reef-examples-spark-(.*?)-shaded\.jar")
            m = r.search(line)
            old_version = m.group(1)
            changed_str += line.replace(old_version, new_version)
        else:
            changed_str += line
    f.close()

    f = open(file, 'w')
    f.write(changed_str)
    f.close()


"""
Read 'IsSnapshot' from lang/cs/build.props
"""
def read_is_snapshot(file):
    f = open(file, 'r')
    r = re.compile('<IsSnapshot>(.*?)</IsSnapshot>')
    while True:
        line = f.readline()
        if not line:
            break
        if "<IsSnapshot>" in line and "</IsSnapshot>" in line:
            m = r.search(line)
            if(m.group(1)=="true"):
                return True
            else:
                return False
    f.close()

"""
Change lang/cs/build.props for the release branch
"""
def change_build_props(file, is_snapshot):
    changed_str = ""

    f = open(file, 'r')
    r1 = re.compile('<IsSnapshot>(.*?)</IsSnapshot>')
    r2 = re.compile('<SnapshotNumber>(.*?)</SnapshotNumber>')

    while True:
        line = f.readline()
        if not line:
            break
        if "<IsSnapshot>" in line and "</IsSnapshot>" in line:
            old_is_snapshot = r1.search(line).group(1)
            changed_str += line.replace(old_is_snapshot, is_snapshot)
        elif "<SnapshotNumber>" in line and "</SnapshotNumber>" in line:
            old_snapshot_number = r2.search(line).group(1)
            if is_snapshot=="false":
              changed_str += line.replace(old_snapshot_number, "00")
            else:
              changed_str += line.replace(old_snapshot_number, "01")
        else:
            changed_str += line
    f.close()

    f = open(file, 'w')
    f.write(changed_str)
    f.close()

    print file

"""
Change the name of shaded.jar in run.cmd and lang/cs/Org.Apache.REEF.Client/Properties/Resources.xml
"""
def change_shaded_jar_name(file, new_version):
    changed_str = ""

    f = open(file, 'r')
    r1 = re.compile('reef-bridge-java-(.*?)-shaded.jar')
    r2 = re.compile('reef-bridge-client-(.*?)-shaded.jar')
    while True:
        line = f.readline()
        if not line:
            break
        m1 = r1.search(line)
        m2 = r2.search(line)
        if m1 is not None:
            changed_str += line.replace(m1.group(1), new_version)
        elif m2 is not None:
            changed_str += line.replace(m2.group(1), new_version)
        else:
            changed_str += line
    f.close()

    f = open(file, 'w')
    f.write(changed_str)
    f.close()

"""
Change the version in Doxyfile
"""
def change_project_number_Doxyfile(file, new_version):
    changed_str = ""

    f = open(file, 'r')
    while True:
        line = f.readline()
        if not line:
            break

        if "PROJECT_NUMBER         = " in line:
            r = re.compile('= (.*?)$')
            m = r.search(line)
            old_version = m.group(1)
            changed_str += line.replace(old_version, new_version)
        else:
            changed_str += line
    f.close()

    f = open(file, 'w')
    f.write(changed_str)
    f.close()

def change_project_number_readme(file, new_version):
    changed_str = ""

    markers = ["<Reference", "<HintPath>"]
    #                             0   .  15etc
    version_regex = re.compile(r'[0-9]\.[0-9]+\.[0-9]+')

    f = open(file, 'r')
    for line in f:
        if any(marker in line for marker in markers):
            changed_str += version_regex.sub(new_version, line, count=1)
        else:
            changed_str += line
    f.close()

    f = open(file, 'w')
    f.write(changed_str)
    f.close()

"""
Change version of every pom.xml, SharedAssemblyInfo.cs,
AssemblyInfo.cpp, run.cmd and Resources.xml
"""
def change_version(reef_home, new_version, pom_only):
    if pom_only:
        for fi in get_filepaths(reef_home):
            if "pom.xml" in fi:
                print fi
                change_pom(fi, new_version)

    else:
        for fi in get_filepaths(reef_home):
            if "pom.xml" in fi:
                print fi
                change_pom(fi, new_version)
            if "SharedAssemblyInfo.cs" in fi:
                print fi
                change_assembly_info_cs(fi, new_version)

        change_assembly_info_cs(reef_home + "/lang/cs/Org.Apache.REEF.Bridge/AssemblyInfo.cpp", new_version)
        print reef_home + "/lang/cs/Org.Apache.REEF.Bridge/AssemblyInfo.cpp"

        change_assembly_info_cs(reef_home + "/lang/cs/Org.Apache.REEF.ClrDriver/AssemblyInfo.cpp", new_version)
        print reef_home + "/lang/cs/Org.Apache.REEF.ClrDriver/AssemblyInfo.cpp"

        change_constants_cs(reef_home + "/lang/cs/Org.Apache.REEF.Driver/DriverConfigGenerator.cs", new_version)
        print reef_home + "/lang/cs/Org.Apache.REEF.Driver/DriverConfigGenerator.cs"

        change_shaded_jar_name(reef_home + "/lang/cs/Org.Apache.REEF.Client/Properties/Resources.xml", new_version)
        print reef_home + "/lang/cs/Org.Apache.REEF.Client/Properties/Resources.xml"

        change_shaded_jar_name(reef_home + "/lang/cs/Org.Apache.REEF.Client/run.cmd", new_version)
        print reef_home + "/lang/cs/Org.Apache.REEF.Client/run.cmd"

        change_project_number_Doxyfile(reef_home + "/Doxyfile", new_version)
        print reef_home + "/Doxyfile"

        change_dotnet_props_cs(reef_home + "/lang/cs/build.DotNet.props", new_version)
        print reef_home + "/lang/cs/build.DotNet.props"

        change_reef_on_spark_scala(reef_home + "/lang/scala/reef-examples-scala/src/main/scala/org/apache/reef/examples/hellospark/ReefOnSpark.scala", new_version)
        print reef_home + "/lang/scala/reef-examples-scala/src/main/scala/org/apache/reef/examples/hellospark/ReefOnSpark.scala"

        helloreef_readme = "/lang/cs/Org.Apache.REEF.Examples.HelloREEF/Readme.md"
        change_project_number_readme(reef_home + helloreef_readme, new_version)
        print reef_home + helloreef_readme

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script for changing REEF version in all files that use it")
    parser.add_argument("reef_home", type=str, help="REEF home")
    parser.add_argument("reef_version", type=str, help="REEF version")
    parser.add_argument("-s", "--isSnapshot", type=str, metavar="<true or false>", help="Change 'IsSnapshot' to true or false", required=True)
    parser.add_argument("-p", "--pomonly", help="Change only poms", action="store_true")
    args = parser.parse_args()

    reef_home = os.path.abspath(args.reef_home)
    reef_version = args.reef_version
    is_snapshot = args.isSnapshot
    pom_only = args.pomonly

    if is_snapshot is not None and not pom_only:
        change_build_props(reef_home + "/lang/cs/build.props", is_snapshot)

    if is_snapshot=="true":
        reef_version += "-SNAPSHOT"

    change_version(reef_home, reef_version, pom_only)

