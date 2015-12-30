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
#Introduction to REEF

It is instructive to first remind us of how MapReduce works: Hadoop Map-Reduce schedules compute tasks on containers for executing map and reduce functions on record data. The basic structure of a Map-Reduce job is as follows. For each input block, schedule a map task that passes each internal record to a user-defined map function and materializes the output in key-sorted order. Further, assign a user-defined number of reduce tasks to non-overlapping portions of the key-space from the map output, then shuffle it across the network to where the respective reduce task is scheduled. For each reduce task, perform a global key-based sort on the shuffled data, group it by key and call the reduce function on each record group; storing the output in a durable fashion (i.e., HDFS).

From the perspective of the scheduler, a number of issues arise that must be appropriately handled in order to scale-out to massive datasets. First, each map task should be scheduled close to where the input block resides; ideally on the same machine or rack. Second, failures can occur at the task level at any step; requiring backup tasks to be scheduled or the job being aborted. Third, performance bottlenecks can cause an imbalance in the task-level progress. The scheduler must react to these stragglers by scheduling clones and incorporating the logical task that crosses the finish line first.

Anyone of these issues can limit the scale-out degree of a Map-Reduce job. In what follows, we prescribe a scheduler framework that provides task life-cycle management mechanisms. Using this framework, we developed a complete version of the Map-Reduce runtime that addresses the above issues. Our framework is designed around three components.

1. `Driver`: captures the client code embodying the scheduler
2. `Evaluator`: provides a runtime environment on a container.
3. `Task`: encapsulates the task-level client code to be executed in an Evaluator.

Below, we describe the client facing interfaces to these components. The core REEF control flow design is based on the reactive extensions (Rx), which enforce asynchronous message-passing method signatures. In Rx terms, interfaces are based on an observer pattern, which expose methods that accept messages from a (possibly remote) asynchronous caller.

<br></br>

<div style="text-align:center" markdown="1">
      <img src="reef-architecture.png"></img>
</div>

*A running REEF job with two `RunningEvaluators` and one `RunningTask`*

<br></br>

The above figure presents the REEF components in terms of a running application, which is written in terms of a `Driver` and task-specific `Task` modules. The application code is packaged and submitted to a REEF client API, which in turn submits a REEF-AM configuration to YARN. The REEF-AM contains a runtime for launching the `Driver` and client libraries for requesting `Evaluators` and launching `Tasks`. When a request for `Evaluators` is made, the REEF-AM negotiates containers with the YARN-RM and launches an `Evaluator` runtime on the YARN-NM that hosts the allocated container. In turn, the `Driver` is given an `Evaluator` object reference, which it uses to submit an `Task`. The `Driver` is also given a `Task` object reference, which it may use to send messages to the `Task` running in the `Evaluator`. The REEF layer implements these communication channels and encodes the computational life-cycle as state transitions, which are surfaced to the `Driver` in the form of Rx messages.

###Computational Life-Cycle

<br></br>

<div style="text-align:center" markdown="1">
      <img src="states-horizontal.png"></img>
</div>

*States of `Evaluator`, `Contexts`, and `Tasks`*

<br></br>

The Figure above describes the state transitions for (a) `Evaluator` and `Context` and (b) `Task` components. Each state transition is associated with an object reference that is surfaced to the `Driver` in an Rx-style interface. For instance, when the YARN-RM notifies the REEF-AM of an allocated container, the `Driver` is given an `AllocatedEvaluator` object; containing methods for adding configurations (i.e., for data services, see below) and file resources, and submit methods that bootstraps the `Evaluator` runtime on the YARN-NM. When an `Evaluator` bootstrap successfully completes, the `Driver` is given an `ActiveContext` object, which can be used to launch `Tasks` or to initiate a close, which triggers a shutdown event at the `Evaluator` runtime and a subsequent container release at the YARN-RM. If at any point a failure occurs, the `Driver` is passed a `FailedEvaluator` object; containing an exception trace when possible.

Recall that the `Driver` launches a `Task` on a submit method call from the `ActiveContext` reference. This state transition is denoted in the Figure above by the edge labeled submit; spanning the two state machines. The REEF-AM passes a `RunningTask` object to the `Driver` after receiving confirmation of a successful `Task` start or resume. The `Driver` may use the `RunningTask` reference to close or suspend the execution; triggering a `CompletedTask` or `SuspendedTask` object reference to the `Driver`. The `SuspendedTask` object contains a memento used to resume the execution on some (possibly alternative) `ActiveContext`. Exceptions during the `Task` execution are surfaced to the `Driver` in the form of a `FailedTask`, which contains the actual exception object.

###Task Component

A `Task` encapsulates the task work of a job. The client interface contains a single synchronous call method that takes an optional memento argument and returns a byte array, which will be packaged with the `CompletedTask` object surfaced to the `Driver`. An exception may be thrown at any point during the call method; returning control back to the `Evaluator`, which packages the exception and sends it to the `Driver` where it is surfaced as a `FailedTask`. The `Evaluator` periodically performs a heartbeat with the REEF-AM to convey its status information. A `Task` can optionally implement a method interface that, when called, returns a (bounded) byte array, which the `Evaluator` includes in its heartbeat to the REEF-AM and surfaced to the `Driver`.