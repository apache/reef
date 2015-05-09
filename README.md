Apache REEF
===========

REEF (Retainable Evaluator Execution Framework) is a scale-out computing fabric that makes it easier to write Big Data applications on top of resource managers (e.g., Apache YARN and Mesos). For example, Azure Stream Analytics is built on REEF and Hadoop. Apache REEF is currently undergoing [incubation](http://reef.incubator.apache.org/) at the [Apache Software Foundation](http://www.apache.org/).

Online Documentation
--------------------

You can find the latest REEF documentation, including tutorials, on the [project web page](http://reef.incubator.apache.org/). This README file contains only basic setup instructions.

Building REEF
-------------

Requirements

* Java 7 Development Kit
* [Apache Maven](http://maven.apache.org) 3 or newer. Make sure that mvn is in your PATH.
* [Protocol Buffers](https://code.google.com/p/protobuf/) Compiler (protoc) 2.5. Make sure that `protoc` is in your PATH.

Java REEF is built using Apache Maven. To build REEF and its example programs, run:

    mvn -DskipTests clean install
    
.Net REEF is build using msbuild, Which internally trigers the maven build for Java:

    msbuild $REEFSourcePath\lang\cs\Org.Apache.REEF.sln /p:Configuration="Release" /p:Platform="x64"

More detailed documentation is available from the [project wiki](https://cwiki.apache.org/confluence/display/REEF/Compiling+REEF)