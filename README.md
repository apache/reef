Wake
====
Wake is an event-driven framework based on ideas from SEDA, Click, Akka and Rx.


Background
----------

Wake targets scalable, high-performance applications.  Wake applications consist of asynchronous *event handlers* that run inside of *stages* that provide scheduling primitives such as thread pool sizing and performance isolation between event handlers.  In addition to event handler and stage APIs, Wake include profiling tools, and a rich standard library of primitives for system builders.

Event driven processing frameworks improve upon the performance of threaded architectures in two ways: (1) Event handlers often have lower memory and context switching overhead than threaded solutions, and (2) event driven systems allow applications to allocate and monitor computational and I/O resources in an extremely fine-grained fashion.  Modern threading packages have done much to address the first concern, and have significantly lowered concurrency control and other overheads in recent years.  However, fine grained resource allocation remains a challenge in threaded systems, and is Wake's primary advantage over other approaches.

Early event driven systems such as SEDA executed each event handler in a dedicated thread pool called a stage.  This isolated low-latency event handlers (such as cache lookups) from expensive high-latency operations, such as disk I/O.  With a single thread pool, long-running I/O operations can easily saturate the thread pool, causing all of the CPUs to block on disk I/O, even when there is more computation to be scheduled.  With separate thread pools, the operating system schedules I/O requests and computation separately, guaranteeing that worker threads with available comptuations will not block on I/O requests.

This is in contrast to event-driven systems such as the CLICK modular router that were designed to maximize throughput for predictable, low latency event-handlers.  When possible, CLICK agressively chains event handlers together, reducing the cost of an event dispatch to that of a function call, and allowing the compiler to perform optimizations such as inlining and constant propagation across event handlers.

Wake allows developers to trade off between these two extremes by explicitly partitioning their event handlers into stages.

Although event handling systems improve upon threaded performance in theory, they are notoriously difficult to reason about.  We kept this in mind while designing Wake, and have gone to great pains to ensure that its APIs are simple and easy to implement without sacrificing our performance goals.

Other event driven systems provide support for so-called "push based" and "pull based" event handlers.  Push-based event systems have event sources invoke event handlers that are exposed by destinations, while pull-based APIs have the destination code invoke iterators to obtain the next available event.  Wake is completely push based.  This eliminates the need for push and pull based variants of event handling logic, and also allowed us to unify all error handling in Wake into a single API.

Systems such as Rx allow event handlers to be dynamically registered and torn down at runtime, allowing applications to evolve over time.  This leads to complicated setup and teardown protocols, where event handlers need to reason about the state of upstream and downstream handlers, both during setup and teardown, but also when routing messages at runtime.  It also encourages design patterns such as dynamic event dispatching that break standard compiler other programming language optimizations.  In contrast, Wake applications consist of immutable graphs of event handlers that are built up from sink to source.  This ensures that, once an event handler has been instantiated, all downstream handlers are ready to receive messages.

Wake is designed to work with [Tang](https://github.com/Microsoft-CISL/Tang/), a dependency injection and configuation management system that focuses on static checking and debuggability.  This makes it extremely easy to wire up complicated graphs of event handling logic.  In addition to making easy to build up event-driven applications, Tang provides a range of configuration procesing and static analysis utilities.  It also provides some simple aspect-style programming primitives that allowed us to implement latency and throughput profilers for Wake.


Core API
--------

Helper libraries

Stage implementations

Profiling

Debugging

THIRD PARTY SOFTWARE 
--------------------
This software is built using Maven.  Maven allows you
to obtain software libraries from other sources as part of the build process.
Those libraries are offered and distributed by third parties under their own
license terms.  Microsoft is not developing, distributing or licensing those
libraries to you, but instead, as a convenience, enables you to use this
software to obtain those libraries directly from the creators or providers.  By
using the software, you acknowledge and agree that you are obtaining the
libraries directly from the third parties and under separate license terms, and
that it is your responsibility to locate, understand and comply with any such
license terms.  Microsoft grants you no license rights for third-party software
or libraries that are obtained using this software.

The list of libraries pulled in this way includes, but is not limited to:

 * asm:asm:jar:3.3.1:compile
 * cglib:cglib:jar:2.2.2:compile
 * com.google.code.findbugs:jsr305:jar:1.3.9:compile
 * com.google.guava:guava:jar:11.0.2:compile
 * com.google.protobuf:protobuf-java:jar:2.5.0:compile
 * commons-cli:commons-cli:jar:1.2:compile
 * commons-configuration:commons-configuration:jar:1.9:compile
 * commons-lang:commons-lang:jar:2.6:compile
 * commons-logging:commons-logging:jar:1.1.1:compile
 * dom4j:dom4j:jar:1.6.1:compile
 * io.netty:netty:jar:3.5.10.Final:compile
 * javax.inject:javax.inject:jar:1:compile
 * junit:junit:jar:4.10:test
 * org.hamcrest:hamcrest-core:jar:1.1:test
 * org.javassist:javassist:jar:3.16.1-GA:compile
 * org.reflections:reflections:jar:0.9.9-RC1:compile
 * xml-apis:xml-apis:jar:1.0.b2:compile


An up-to-date list of dependencies pulled in this way can be generated via `mvn dependency:list` on the command line.
