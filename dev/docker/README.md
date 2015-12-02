Apache REEF&trade; Docker Tests
====================================

[Apache REEF](http://reef.apache.org/)&trade; is
an approach to simplify and unify the lower layers of big data systems
on modern resource managers. This project maintains docker images
to simulate the target underlying systems as similar as possible.

Docker-based Test Cluster
-------------------------

| Environment | OS           | Hadoop Version            | Description   | alias     |
|-------------|--------------|---------------------------|---------------|-----------|
| YARN        | Ubuntu 12.04 | HDP 2.1.15 (Hadoop 2.4.0) | HDInsight 3.1 | hdi3.1    |
| YARN        | Ubuntu 12.04 | HDP 2.2.7  (Hadoop 2.6.0) | HDInsight 3.2 | hdi3.2    |
| YARN        | Ubuntu 12.04 | HDP 2.2.8  (Hadoop 2.6.0) |               | hdp2.2    |
| YARN        | Ubuntu 12.04 | HDP 2.3.2  (Hadoop 2.7.1) |               | hdp2.3    |
| YARN        | Ubuntu 12.04 | Apache Hadoop 2.7.1       |               | apache2.7 |
| MESOS       | Ubuntu 12.04 | Apache Mesos 0.24.1       |               | mesos0.24 |
| MESOS       | Ubuntu 12.04 | Apache Mesos 0.25.0       |               | mesos0.25 |

Please note that all images use Oracle JDK 7u80.

Requirements
------------

First, download and *build* Apache REEF. You can find a guide for this
[here](https://cwiki.apache.org/confluence/display/REEF/Compiling+REEF).
Second, set $REEF_HOME to the directory of Apache REEF for the script to know its location.
Tests will use `$REEF_HOME/lang/java/reef-tests/target/reef-tests-*-test-jar-with-dependencies.jar`.

Build docker images (Optional)
------------------------------

This step is optional. We provide pre-built docker images in DockerHub.
Please go to next step if you do not want to build images.
To build all images manually, run `build.sh`.

```sh
$ ./build.sh
```

Run a docker-based cluster
--------------------------

Run `run-cluster.sh reefrt/{alias}`. It will create a small cluster
with 4 nodes and run a shell on the master node.
For all cluster, Apache Hadoop (HDFS/YARN) is installed. The hostnames
are `hnn-001-01` (master) and `hnn-001-01 ~ hnn-001-03` (slaves).

```sh
$ ./run-cluster.sh reefrt/hdi3.2
$ root@hnn-001-01:~#
```

Test Apache REEF on a docker-based YARN cluster
-----------------------------------------------

Among reefrt/hdi3.1, reefrt/hdi3.2, reefrt/hdp2.2, reefrt/hdp2.3, and reefrt/apache2.7,
choose one and run it as described in section _Run a docker-based cluster_.
If you want to test on Hadoop 2.7.1, choose reefrt/apache2.7.
To simulate HDInsight 3.2, choose reefrt/hdi3.2.
After running a cluster, use the following commands.

```sh
$ root@hnn-001-01:~# cd /reef
$ root@hnn-001-01:~# ./bin/runyarntests.sh
...
OK (# tests)
$ root@hnn-001-01:~# exit
```

If you see 'OK', it means all tests are passed.
`exit` command will terminate your cluster automatically.

Test Apache REEF on a docker-based Mesos cluster
-----------------------------------------------

Among reefrt/mesos0.24 and reefrt/mesos0.25,
choose one and run it as described in section _Run a docker-based cluster_.
If you want to test on Mesos 0.25, choose reefrt/mesos0.25.
After running a cluster, use the following commands.

```sh
$ root@hnn-001-01:~# cd /reef
$ root@hnn-001-01:~# ./bin/runmesostests.sh hnn-001-01:5050
...
OK (# tests)
$ root@hnn-001-01:~# exit
```

If you see 'OK', it means all tests are passed.
`exit` command will terminate your cluster automatically.

Reference
---------
* Ubuntu OS: https://hub.docker.com/_/ubuntu/
* Java: https://github.com/dockerfile/java/tree/master/oracle-java7
* Hadoop: https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/DockerContainerExecutor.html
* Hadoop: https://github.com/sequenceiq/hadoop-docker
* Mesos: https://mesos.apache.org/gettingstarted
* Prebuilt Mesos: https://mesosphere.com/downloads

