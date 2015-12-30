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
#Glossary

- [Context](#context)
- [Driver](#driver)
- [Evaluator](#evaluator)
- [Task](#task)

###<a name="context"></a>Context

Contexts are a way to structure the state and Configuration of an Evaluator. A Context exists on one and only one individual Evaluator. Each Evaluator has at least one Context, which we refer to as the *root* Context. This root context is special, as it is *required* on all Evaluators and because closing it is synonymous to releasing the Evaluator. In many simple REEF programs, this is the only Context used and it is therefore convenient to think of it as synonymous to "the Evaluator": The Driver can submit Tasks to it, is notified when they are done and can close the root context when it wants to dispose of the Evaluator.

Contexts are formed by calls to `submitContext()` to the event types that allow this (`AllocatedEvaluator` and `ActiveContext`) Contexts are the main way for an Evaluator to be exposed and accessed. For instance, Tasks are submitted to an `ActiveContext` which represents the top Context on the Evaluator.

Beyond this, a Driver can submit a Context to the root, or in fact any, Context, as long as the resulting structure is that of a stack: The root Context forms the bottom of the stack, the top-most Context is called *active*, hence the `ActiveContext` event. The two can be one and the same, and often are: The root Context is the subject of the first `ActiveContext` event on an Evaluator.

Nomenclature: When Context B is submitted to an already existing Context A, we say that Context A is the parent Context of Context B. Also, Context B is the child of Context A.

It is only the `ActiveContext` that allows the submission of Tasks or child Contexts. Hence, one can think of the whole Evaluator structure as that of a stack: the root Context at the bottom, layers of Contexts in the middle and either the current `ActiveContext` or the current Task at the top.

####Objects and Configuration: What's in a Context?

It is convenient to think of a Context as a `Configuration` that gets merged with the `Configuration` supplied for Tasks and child Contexts. While not entirely true (see below), this view allows us to show just *why* Contexts are a convenient construct.

It is often the case that subsequent tasks that get executed on an Evaluator want access to the same Configuration variables and / or the same objects. Consider a simple `LinkedList` bound to a named parameter. If that linked list is part of the subsequent Task `Configurations` submitted, each Task will get its very *own* `LinkedList`. If the named parameter is bound in the Context `Configuration`, all Tasks subsequently submitted to the Context will get the very *same* `LinkedList` instance.

####Contexts are (Tang) Injectors

This mechanism is implemented by using Tang's `Injector`s. On the Evaluator, a Task is launched by first *forking* the Context's `Injector` with the Task `Configuration` and then requesting an instance of the `Task` interface from that forked `Injector`. By this mechanism and the fact that objects are singletons with respect to an `Injector` in Tang, object sharing can be implemented. All objects already instantiated on the Context `Injector` will also be referenced by the Task `Injector`. Hence, the `LinkedList` in the example above would be shared amongst subsequent Task `Injectors` in the construction of the `Task` instance.

###<a name="driver"></a>Driver

REEF imposes a centralized control flow design on applications: All events are routed to the master node, called the Driver. REEF also prescribes event-driven programming for the Driver. In that sense, the application provided Driver is a collection of event handlers for the various events exposed in `DriverConfiguration`. While most of these deal with occurrences during the computation (Evaluator allocation, Task launch, ...), several stand out as life-cycle events of the Driver, and therefore the application:

####ON_START

This event is triggered by REEF when the Driver is ready to start executing. At this point communication with the Resource Manager has been established, all event handlers have been instantiated and the event graph in the Driver was deemed to be complete enough to start. In a typical application, this is when the Driver requests the first set of Evaluators.

####ON_STOP

This event is fired right before the Driver shuts down. REEF determines Driver shutdown by proof that no future events can happen:

- The Clock has no outstanding alarms.
- The resource manager has no outstanding requests.
- The application has no Evaluators allocated anymore.

Hence, the `ON_STOP` event can be used to prevent Driver shutdown, e.g. in applications that need to wait for external triggers (e.g. a UI) to proceed. To do so, one would schedule an alarm in the `ON_STOP` handler.

###<a name="evaluator"></a>Evaluator

####Evaluators and Tasks

The Evaluator is the runtime environment for Tasks. On one Evaluator, there is either no or one Task executing at any given point in time. Different or multiple executions of the same Tasks can be executed in sequence on an Evaluator. The Evaluator and Task lifecycle are decoupled: Whenever a Task finishes, the Driver receives the CompletedTask event, which contains a reference to the Evaluator the Task executed on. It is then up to the Driver to decide whether to return the Evaluator to the resource manager or to make other use of it, e.g. by submitting another task.

####Evaluators and Contexts

Contexts are REEF's form of state management inside of the Evaluator. See the [Context](#context) section for more information.

####Evaluators and the Resource Manager

On typical resource managers, an Evaluator is a process executing inside a container. Depending on the resource manager, that process may or may not be guarded by a resource or security isolation layer.

This also means that the Evaluator, not the Task, is the unit of resource consumption: while an Evaluator is occupying a Container, that Container is "used" from the perspective of the Resource Manager. That is true even if the Evaluator is idle from the perspective of the Driver, i.e. when no Task is running on it.

###<a name="task"></a>Task

####Definition

A Task in REEF is a unit of work to be executed on an Evaluator. In its simplest form, a Task is merely an object implementing the Task interface which prescribes a single method:

    public byte[] call(byte[] input);
    
From REEF's perspective, a Task is therefore a single threaded method call. It starts when entering the call method. It is a `RunningTask` while it hasn't returned from it and is a `CompletedTask` when it has. Should there be an Exception thrown by `call()`, we call it a `FailedTask`.

Task identity is established by a user-defined string set in `TaskConfiguration.IDENTIFIER`. All subsequent task-related events in the Driver will carry that ID. Note that REEF doesn't take any particular precautions to ensure unique Task identifiers. It is up to the application to do so. While technically feasible to assign the same identifier to multiple Tasks, this isn't advised as it makes error handling, debugging and logging unnecessarily hard.

####Inputs and outputs of a Task

The return value of the `call` method will be made available to the Driver as part of the `CompletedTask` event. Note that it isn't advised to return large values in this fashion, but merely small control flow or status information. Sending large data on this channel creates the risk of overloading the Driver at scale. The networking APIs provided by REEF IO are much better suited for data transmissions than this channel.

The parameter given to the call method is also to be used in a similar fashion: The Driver passes its value as part of the Task submission. It is meant e.g. to convey a restart point for the task. Note that the same functionality can now be better provided by Tang and a constructor parameter.

####Communicating between a Task and a Driver

REEF provides some facilities to communicate between a Driver and a Task. These mostly stem from allowing application code to "free-ride" on REEF's control flow channels such as the heartbeat between the Evaluator and the Task.

#####Sending a message from the Driver to a Task

REEF maintains a heartbeat between any Evaluator and the Driver. There are two ways by which a heartbeat can be triggered.

- Upon some schedule (which may also vary at runtime due to load conditions on the Driver), each Evaluator will report its current status to the Driver. This is used by the Driver to maintain health status and load statistics of the Evaluators.

- Whenever the status of the Evaluator changes (e.g. when a Task completes), a Heartbeat is triggered immediately.

Whenever the Evaluator performs a heartbeat, it will ask the Task whether it has any message to share with the Driver by inquiring the class registered in `TaskConfiguration.ON_SEND_MESSAGE`. It is wise for that message to be small, as we otherwise run the risk of overwhelming the Driver with heartbeat traffic at scale.

####Multithreaded Tasks

Just because REEF views a Task as a method call doesn't restrict the Task to be single threaded. A Task is free to spawn threads in the course of its execution. However, a Task that does so needs to take care of a few considerations:

- All Threads spawned need to exit before the `Task.call()` method returns. Otherwise, you run the risk of resource leakage.

- Exceptions on spawned Threads need to be caught and re-thrown by the `Thread.call()` method. Before that, all spawned threads need to be shut down, just like during a normal exit of `Task.call()`. If an exception from an another thread isn't caught, REEF's JVM level exception handler will catch it and declare a FailedEvaluator. This is inefficient, but not technically wrong: The Driver will then have to allocate another Evaluator and try again.