<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

Building and Testing REEF Java
==================

Prerequisites
-------------

  * Java Development Kit 7 or 8. Make sure that `$JAVA_HOME` points to it.
  * [Apache Maven](https://maven.apache.org/download.cgi) 3.3 or newer.
    Make sure that `mvn` is in your `$PATH` and `$M2_HOME` points to its installation.
  * [Protocol Buffers Compiler version 2.5](https://github.com/google/protobuf/releases/tag/v2.5.0).
    Make sure that `protoc` is on your `PATH`.

Build Instructions
------------

The Java side of REEF is built using Apache Maven. To build and run tests, execute:

    mvn clean install

On an ubuntu vm running version 16.10 the build typically takes 25-30 minutes if you include all tests, this particular vm was a Standard F8 vm with 8 cores and 16 GB of memory

To perform build alone without tests in a multithreaded mode, execute

    mvn -TC1 -DskipTests clean install

To perform "fast" build, which skips tests and all code quality enforcement tools, execute:

    mvn clean install -DskipTests -TC1 -P!code-quality


  * Please follow the following wiki for all running all java builds [Java Build Wiki](https://cwiki.apache.org/confluence/display/REEF/Linux), the REEF unit tests require a number of open files which is greater than the default open file limit on a number of Linux distributions such as Ubuntu 16.04/16.10.  This limit is controlled in the shell by the "ulimit -n" command

  * The builds for REEF have been tested on Ubuntu versions 16.04 and 16.10, Windows 10, Windows Server 2012R2, Windows Server 2016 and Mac OS


Test Instructions
------------

To run tests separately on local runtime, execute:

    mvn test

Note that the tests will take several minutes to complete. You will
also see stack traces fly by. Not to worry: those are part of the
tests that test REEF's error reporting.

Code Quality Enforcement Tools
------------

Java build incorporates several code quality tools:

* **Apache RAT** verifies that Apache headers are in place where needed. To run this check separately, execute:

  `mvn apache-rat:check`

* **Checkstyle** verifies that all Java code adheres to a coding standard. To run this check separately, execute:

  `mvn checkstyle:checkstyle`

  Per-project Checkstyle reports can be found at `\<project>\target\site\checkstyle.html`.  Violations which caused the build break will show up as errors and need to be fixed; violations which show up as warnings or info can be ignored.

* **Findbugs** looks for potential bugs in Java code, and can fail the build if any high-priority warnings are found.
  To run this check separately, execute:

  `mvn findbugs:check xml:transform`

  Per-project Findbugs reports can be found at `\<project>\target\findbugs\findbugsXml.html`.

Continuous Integration
------------

We use [Travis CI](https://travis-ci.org/) to run continuous integration for REEF Java code (i.e. build and run tests
for all pull requests and commits to master branch).

It can be convenient to set up Travis for your fork of REEF repository, for example, to reproduce a test failure which
can't be reproduced locally.

1. Log in to [Travis CI](https://travis-ci.org/) using your GitHub credentials.
2. Go to [your profile](https://travis-ci.org/profile/) and switch reef repository to "on".
3. Fine-tune configuration as required at Settings tab of repository; you can enable building pushes to your repository
   when you're investigating something and disable them when you don't need them.
4. Edit [Travis configuration file](../../.travis.yml) as required (by default you'll use the same configuration as REEF build).
