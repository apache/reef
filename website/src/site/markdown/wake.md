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
Wake
====

Wake is an event-driven framework based on ideas from SEDA, Click, Akka and Rx.  It is *general purpose* in the sense that it is designed to support computationally intensive applications as well as high performance networking, storage, and legacy I/O systems.  We implemented Wake to support high-performance, scalable analytical processing systems ("big data" applications), and have used it to implement control plane logic (which requires high fanout and low latency) and the data plane (which requires high-throughput processing as well).


Background
----------
Wake applications consist of asynchronous *event handlers* that run inside of *stages*.  Stages provide scheduling primitives such as thread pool sizing and performance isolation between event handlers.  In addition to event handler and stage APIs, Wake includes profiling tools and a rich standard library of primitives for system builders.

Event driven processing frameworks improve upon the performance of threaded architectures in two ways: (1) Event handlers often have lower memory and context switching overhead than threaded solutions, and (2) event driven systems allow applications to allocate and monitor computational and I/O resources in an extremely fine-grained fashion.  Modern threading packages have done much to address the first concern, and have significantly lowered concurrency control and other implementation overheads in recent years.  However, fine grained resource allocation remains a challenge in threaded systems, and is Wake's primary advantage over threading.

Early event driven systems such as SEDA executed each event handler in a dedicated thread pool called a stage.  This isolated low-latency event handlers (such as cache lookups) from expensive high-latency operations, such as disk I/O.  With a single thread pool, high-latency I/O operations can easily monopolize the thread pool, causing all of the CPUs to block on disk I/O, even when there is computation to be scheduled.  With separate thread pools, the operating system schedules I/O requests and computation separately, guaranteeing that runnable computations will not block on I/O requests.

This is in contrast to event-driven systems such as the Click modular router that were designed to maximize throughput for predictable, low latency event-handlers.  When possible, Click aggressively chains event handlers together, reducing the cost of an event dispatch to that of a function call, and allowing the compiler to perform optimizations such as inlining and constant propagation across event handlers.

Wake allows developers to trade off between these two extremes by explicitly partitioning their event handlers into stages.  Within a stage, event handlers engage in *thread-sharing* by simply calling each other directly.  When an event crosses a stage boundary, it is placed in a queue of similar events.  The queue is then drained by the threads managed by the receiving stage.

Although event handling systems improve upon threaded performance in theory, they are notoriously difficult to reason about.  We kept this in mind while designing Wake, and have gone to great pains to ensure that its APIs are simple and easy to implement without sacrificing our performance goals.

Other event driven systems provide support for so-called *push-based* and *pull-based* event handlers.  In push-based systems, event sources invoke event handlers that are exposed by the events' destinations, while pull-based APIs have the destination code invoke iterators to obtain the next available event from the source.

Wake is completely push based.  This eliminates the need for push and pull based variants of event handling logic, and also allowed us to unify all error handling in Wake into a single API.  It is always possible to convert between push and pull based APIs by inserting a queue and a thread boundary between the push and pull based code.  Wake supports libraries and applications that use this trick, since operating systems and legacy code sometimes expose pull-based APIs.

Systems such as Rx allow event handlers to be dynamically registered and torn down at runtime, allowing applications to evolve over time.  This leads to complicated setup and teardown protocols, where event handlers need to reason about the state of upstream and downstream handlers, both during setup and teardown, but also when routing messages at runtime.  It also encourages design patterns such as dynamic event dispatching that break standard compiler optimizations.  In contrast, Wake applications consist of immutable graphs of event handlers that are built up from sink to source.  This ensures that, once an event handler has been instantiated, all downstream handlers are ready to receive messages.

Wake is designed to work with [Tang](tang.html), a dependency injection system that focuses on configuration and debuggability.  This makes it extremely easy to wire up complicated graphs of event handling logic.  In addition to making it easy to build up event-driven applications, Tang provides a range of static analysis tools and provides a simple aspect-style programming facility that supports Wake's latency and throughput profilers.


Core API
--------

### Event Handlers

Wake provides two APIs for event handler implementations.  The first is the [EventHandler](https://github.com/apache/reef/blob/master/lang/java/reef-wake/wake/src/main/java/org/apache/reef/wake/EventHandler.java) interface:

    public interface EventHandler<T> {
      void onNext(T value);
    }

Callers of `onNext()` should assume that it is asynchronous, and that it always succeeds.  Unrecoverable errors should be reported by throwing a runtime exception (which should not be caught, and will instead take down the process).  Recoverable errors are reported by invoking an event handler that contains the appropriate error handling logic.

The latter approach can be implemented by registering separate event handlers for each type of error.  However, for convenience, it is formalized in Wake's simplified version of the Rx [Observer](https://github.com/apache/reef/blob/master/lang/java/reef-wake/wake/src/main/java/org/apache/reef/wake/rx/Observer.java) interface:
    
    public interface Observer<T> {
      void onNext(final T value);
      void onError(final Exception error);
      void onCompleted();
    }

The `Observer` is designed for stateful event handlers that need to be explicitly torn down at exit, or when errors occor.  Such event handlers may maintain open network sockets, write to disk, buffer output, and so on.  As with `onNext()`, neither `onError()` nor `onCompleted()` throw exceptions.  Instead, callers should assume that they are asynchronously invoked.

`EventHandler` and `Observer` implementations should be threadsafe and handle concurrent invocations of `onNext()`.  However, it is illegal to call `onCompleted()` or `onError()` in race with any calls to `onNext()`, and the call to `onCompleted()` or `onError()` must be the last call made to the object.  Therefore, implementations of `onCompleted()` and `onError()` can assume they have a lock on `this`, and that `this` has not been torn down and is still in a valid state.

We chose these invariants because they are simple and easy to enforce.  In most cases, application logic simply limits calls to `onCompleted()` and `onError()` to other implementations of `onError()` and `onCompleted()`, and relies upon Wake (and any intervening application logic) to obey the same protocol.

### Stages

Wake Stages are responsible for resource management.  The base [Stage](https://github.com/apache/reef/blob/master/lang/java/reef-wake/wake/src/main/java/org/apache/reef/wake/Stage.java) interface is fairly simple:

    public interface Stage extends AutoCloseable { }

The only method it contains is `close()` from auto-closable.  This reflects the fact that Wake stages can either contain `EventHandler`s, as [EStage](https://github.com/apache/reef/blob/master/lang/java/reef-wake/wake/src/main/java/org/apache/reef/wake/EStage.java) implementations do:

    public interface EStage<T> extends EventHandler<T>, Stage { }

or they can contain `Observable`s, as [RxStage](https://github.com/apache/reef/blob/master/lang/java/reef-wake/wake/src/main/java/org/apache/reef/wake/rx/RxStage.java) implementations do:

    public interface RxStage<T> extends Observer<T>, Stage { }

In both cases, the stage simply exposes the same API as the event handler that it manages.  This allows code that produces events to treat downstream stages and raw `EventHandlers` / `Observers` interchangebly.   Recall that Wake implements thread sharing by allowing EventHandlers and Observers to directly invoke each other.  Since Stages implement the same interface as raw EventHandlers and Observers, this pushes the placement of thread boundaries and other scheduling tradeoffs to the code that is instantiating the application.  In turn, this simplifies testing and improves the reusability of code written on top of Wake.

#### close() vs. onCompleted()

It may seem strange that Wake RxStage exposes two shutdown methods: `close()` and `onCompleted()`.  Since `onCompleted()` is part of the Observer API, it may be implemented in an asynchronous fashion.  This makes it difficult for applications to cleanly shut down, since, even after `onCompleted()` has returned, resources may still be held by the downstream code.

In contrast, `close()` is synchronous, and is not allowed to return until all queued events have been processed, and any resources held by the Stage implementation have been released.  The upshot is that shutdown sequences in Wake work as follows:  Once the upstream event sources are done calling `onNext()` (and all calls to `onNext()` have returned), `onCompleted()` or `onError()` is called exactly once per stage.  After the `onCompleted()` or `onError()` call to a given stage has returned, `close()` must be called.  Once `close()` returns, all resources have been released, and the JVM may safely exit, or the code that is invoking Wake may proceed under the assumption that no resources or memory have been leaked.  Note that, depending on the implementation of the downstream Stage, there may be a delay between the return of calls such as `onNext()` or `onCompleted()` and their execution.  Therefore, it is possible that the stage will continue to schedule `onNext()` calls after `close()` has been invoked.  It is illegal for stages to drop events on shutdown, so the stage will execute the requests in its queue before it releases resources and returns from `close()`.

`Observer` implementations do not expose a `close()` method, and generally do not invoke `close()`.  Instead, when `onCompleted()` is invoked, it should arrange for `onCompleted()` to be called on any `Observer` instances that `this` directly invokes, free any resources it is holding, and then return.  Since the downstream `onCompleted()` calls are potentially asynchronous, it cannot assume that downstream cleanup completes before it returns.

In a thread pool `Stage`, the final `close()` call will block until there are no more outstanding events queued in the stage.  Once `close()` has been called (and returns) on each stage, no events are left in any queues, and no `Observer` or `EventHandler` objects are holding resources or scheduled on any cores, so shutdown is compelete.

Helper libraries
----------------

Wake includes a number of standard library packages:

 - `org.apache.reef.wake.time` allows events to be scheduled in the future, and notifies the application when it starts and when it is being torn down.
 - `org.apache.reef.wake.remote` provides networking primitives, including hooks into netty (a high-performance event-based networking library for Java).
 - `org.apache.reef.wake.metrics` provides implementations of standard latency and throughput instrumentation.
 - `org.apache.reef.wake.profiler` provides a graphical profiler that automatically instruments Tang-based Wake applications.