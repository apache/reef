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
#Further REEF Examples

- [Running HelloREEF on YARN](#yarn)
    - [Prerequisites](#yarn-prerequisites)
    - [How to configure REEF on YARN](#yarn-configurations)
    - [How to launch HelloReefYarn](#yarn-launch)
- [Running a REEF Webserver: HelloREEFHttp](#http)
    - [Prerequisites](#http-prerequisites)
    - [HttpServerShellCmdtHandler](#http-server-shell)
- [Task Scheduler: Retaining Evaluators](#task-scheduler)
    - [Prerequisites](#task-scheduler-prerequisites)
    - [REST API](#task-scheduler-rest-api)
    - [Reusing the Evaluators](#task-scheduler-reusing-evaluators)
    

###<a name="yarn"></a>Running HelloREEF on YARN

REEF applications can be run on multiple runtime environments. Using `HelloReefYarn`, we will see how to configure and launch REEF applications on YARN.

####<a name="yarn-prerequisites"></a>Prerequisites

* [You have compiled REEF locally](tutorial.html#install)
* [YARN](http://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html)

####<a name="yarn-configurations"></a>How to configure REEF on YARN

The only difference between running a REEF application on YARN vs locally is the runtime configuration:

```
 final LauncherStatus status = DriverLauncher
        .getLauncher(YarnClientConfiguration.CONF.build())
        .run(getDriverConfiguration(), JOB_TIMEOUT);
```

####<a name="yarn-launch"></a>How to launch HelloReefYarn

Running `HelloReefYarn` is very similar to running `HelloREEF`:

    yarn jar reef-examples/target/reef-examples-{$REEF_VERSION}-shaded.jar org.apache.reef.examples.hello.HelloREEFYarn

**Note**: *The path divider may be different for different OS (e.g. Windows uses \\ while Linux uses / for dividers) so change the code as needed.*

You can see how REEF applications work on YARN environments in [Introduction to REEF](introduction.html).

###<a name="http"></a>Running a REEF Webserver: HelloREEFHttp

REEF also has a webserver interface to handle HTTP requests. This webserver can be utilized in many different manners such as in Interprocess Communcation or in conjuction with the REST API.

To demonstrate a possible use for this interface, `HelloREEFHttp` serves as a simple webserver to execute shell commands requested from user input. The first thing we should do is register a handler to receive the HTTP requests.

####<a name="http-prerequisites"></a>Prerequisites

* [You have compiled REEF locally](tutorial.html#install)

####<a name="http-server-shell"></a>HttpServerShellCmdtHandler

`HttpServerShellCmdtHandler` implements `HttpHandler` but three methods must be overridden first: `getUriSpecification`, `setUriSpecification`, and `onHttpRequest`.

- <a name="http-urispecification"></a>
`UriSpecification` defines the URI specification for the handler. More than one handler can exist per application and thus each handler is distinguished using this specification. Since `HelloREEFHttp` defines `UriSpecification` as `Command`, an HTTP request looks like `http://{host_address}:{host_port}/Command/{request}`.

- <a name="http-onhttprequest"></a>
`onHttpRequest` defines a hook for when an HTTP request for this handler is invoked. 

###<a name="task-scheduler"></a>Retaining Evaluators: Task Scheduler

Another example is Task scheduler. Getting commands from users using the REST API, it  allocates multiple evaluators and submits the tasks.

It is a basic Task Scheduler example using Reef-webserver. The application receives the task (shell command) list from user and execute the tasks in a FIFO order.

####<a name="task-scheduler-prerequisites"></a>Prerequisites

* [You have compiled REEF locally](tutorial.html#install)
* [Running REEF Webserver : HelloREEFHttp](#http)

####<a name="task-scheduler-rest-api"></a>REST API

Users can send the HTTP request to the server via URL : 

    http://{address}:{port}/reef-example-scheduler/v1

And the possible requests are as follows:

* `/list`: lists all the tasks' statuses.
* `/clear`: clears all the tasks waiting in the queue and returns how many tasks have been removed.
* `/submit?cmd=COMMAND`: submits a task to execute COMMAND and returns the task id.
* `/status?id=ID`: returns the status of the task with the id, "ID".
* `/cancel?id=ID`: cancels the task with the id, "ID".
* `/max-eval?num={num}`: sets the maximum number of evaluators.

The result of each task is written in the log files - both in the driver's and the evaluators'.

####<a name="task-scheduler-reusing-evaluators"></a>Reusing the Evaluators

You can find the method `retainEvaluator()` in SchedulerDriver:

```
  /**
   * Retain the complete evaluators submitting another task
   * until there is no need to reuse them.
   */
  private synchronized void retainEvaluator(final ActiveContext context) {
    if (scheduler.hasPendingTasks()) {
      scheduler.submitTask(context);
    } else if (nActiveEval > 1) {
      nActiveEval--;
      context.close();
    } else {
      state = State.READY;
      waitForCommands(context);
    }
  }
```

When `Task` completes, `EventHandler` for `CompletedTask` event is invoked. An instance of `CompletedTask` is then passed using the parameter to get the `ActiveContext` object from the `CompletedTask`. We can reuse this `Evaluator` by submitting another `Task` to it if there is a task to launch.

Using the `-retain false` argument disables this functionality and allocates a new evalutor for every task.