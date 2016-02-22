[![Build Status](https://travis-ci.org/apache/reef.svg?branch=master)](https://travis-ci.org/apache/reef)

Apache REEF&trade;
========================
Apache REEF&trade; (Retainable Evaluator Execution Framework) is a scale-out
computing fabric that simplifies the development of Big Data
applications on top of resource managers (e.g., Apache YARN and
Mesos). For example, Azure Stream Analytics is built on REEF and
Hadoop.


Online Documentation
====================
This file will cover the very basics of compiling and testing
REEF. Much more detailed information can be found in the following
places:

  * [The project website](http://reef.apache.org/)
  * [The project wiki](https://cwiki.apache.org/confluence/display/REEF/Home)
  
[The developer
mailinglist](http://reef.apache.org/mailing-list.html) is
the best way to reach REEF's developers when the above aren't
sufficient.

Building REEF
=============

Requirements
------------

  * Java 7 Development Kit.
  * [Apache Maven](http://maven.apache.org) 3 or newer. Make sure that
    `mvn` is in your `PATH`.
  * [Protocol Buffers](https://code.google.com/p/protobuf/) Compiler
    version 2.5. Make sure that `protoc` is on your `PATH`.
  * For REEF.NET, you will also need [Visual Studio
    2013](http://www.visualstudio.com). Most REEF developers use the
    free Community Edition.

REEF Java
---------
The Java side of REEF is built using Apache Maven. To build, execute:

    mvn -DskipTests clean install

To test, execute:

    mvn test

Note that the tests will take several minutes to complete. You will
also see stack traces fly by. Not to worry: those are part of the
tests that test REEF's error reporting.

REEF.NET
--------
REEF.NET uses REEF Java. In fact, the instructions below build REEF
Java as part of building REEF.NET. Hence, the same requirements apply.

To build and test in Visual Studio, open
`lang\cs\Org.Apache.REEF.sln`.

Alternatively, you can build REEF.NET from a developer command line
via:

    msbuild .\lang\cs\Org.Apache.REEF.sln /p:Configuration="Debug" /p:Platform="x64"

To test, execute the following command thereafter:

    msbuild .\lang\cs\TestRunner.proj /p:Configuration="Debug" /p:Platform="x64"

Additional Information
----------------------
More detailed documentation, including building from PowerShell and
creating NuGets is available from the [project
wiki](https://cwiki.apache.org/confluence/display/REEF/Home),
specifically the [building
instructions](https://cwiki.apache.org/confluence/display/REEF/Compiling+REEF).
