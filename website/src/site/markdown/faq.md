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
#FAQ

1. [Who is REEF for?](#who)
2. [Why did it come about?](#why)
3. [What is Tang?](#tang)
4. [What is Wake?](#wake)
5. [How can I get started?](#how)

###<a name="who"></a>Who is REEF for?

REEF is for developers of data processing systems on cloud computing platforms that provide fine-grained resource allocations. REEF provides system authors with a centralized (pluggable) control flow that embeds a user-defined system controller called the Job Driver. The interfaces associated with the Job Driver are event driven; events signal resource allocations and failures, various states associated with task executions and communication channels, alarms based on wall-clock or logical time, and so on. REEF also aims to package a variety of data-processing libraries (e.g., high-bandwidth shuffle, relational operators, low-latency group communication, etc.) in a reusable form. Authors of big data systems and toolkits can leverage REEF to immediately begin development of their application specific data flow, while reusing packaged libraries where they make sense.

___________________________________________________________________________

###<a name="why"></a>Why did it come about?

Traditional data-processing systems are built around a single programming model (like SQL or MapReduce) and a runtime (query) engine. These systems assume full ownership over the machine resources used to execute compiled queries. For example, Hadoop (version one) supports the MapReduce programming model, which is used to express jobs that execute a map step followed by an optional reduce step. Each step is carried out by some number of parallel tasks. The Hadoop runtime is built on a single master (the JobTracker) that schedules map and reduce tasks on a set of workers (TaskTrackers) that expose fixed-sized task "slots". This design leads to three key problems in Hadoop: 


1. The resources tied to a TaskTracker are provisioned for MapReduce only.
2. Clients must speak some form of MapReduce in order to make use of cluster resources, and in turn, gain compute access to the data that lives there.
3. Poor cluster utilization, especially in the case of idle resources (slots) due to straggler tasks.

With YARN (Hadoop version two), resource management has been decoupled from the MapReduce programming model in Hadoop, freeing cluster resources from slotted formats, and opening the door to programming frameworks beyond MapReduce. It is well understood that while enticingly simple and fault-tolerant, the MapReduce model is not ideal for many applications, especially iterative or recursive workloads like machine learning and graph processing, and those that tremendously benefit from main memory (as opposed to disk based) computation. A variety of big data systems stem from this insight: Microsoft's Dryad, Apache Spark, Google's Pregel, CMU's GraphLab and UCI's AsterixDB, to name a few. Each of these systems add unique capabilities, but form islands around key functionalities, making it hard to share both data and compute resources between them. YARN, and related resource managers, move us one step closer toward a unified Big Data system stack. The goal of REEF is to provide the next level of detail in this layering.

______________________________________________________

###<a name="tang"></a>What is Tang?

Tang is a dependency injection Framework co-developed with REEF. It has extensive documentation which can be found [here](tang.html).

________________________________________________

###<a name="wake"></a>What is Wake?

Please refer to [this](wake.html) section.

________________________________________________


###<a name="how"></a>How can I get started?

Check out the [REEF Tutorial](https://cwiki.apache.org/confluence/display/REEF/Tutorials) and the [Contributing](https://cwiki.apache.org/confluence/display/REEF/Contributing) page and join the community!