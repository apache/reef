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

To perform build alone without tests in a multithreaded mode, execute

    mvn -TC1 -DskipTests clean install

To perform "fast" build, which skips tests and all code quality enforcement tools, execute:

    mvn clean install -DskipTests -TC1 -P!code-quality

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

