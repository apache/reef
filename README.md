# Apache REEF

REEF (Retainable Evaluator Execution Framework) is a scale-out computing fabric
that makes it easier to write Big Data applications on top of resource managers
(e.g., Apache YARN and Mesos). For example, Azure Stream Analytics is built on
REEF and Hadoop. Apache REEF is currently undergoing incubation at the [Apache
Software Foundation](http://www.apache.org/).

<http://reef.incubator.apache.org/>

## Online Documentation

You can find the latest REEF documentation, including tutorials, on the
[project web page](http://reef.incubator.apache.org/). This README file
contains only basic setup instructions.

## Building REEF

REEF is built using [Apache Maven](http://maven.apache.org/).
To build REEF and its example programs, run:

    mvn -DskipTests clean install

More detailed documentation is available from the project site.

