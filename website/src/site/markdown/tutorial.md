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
#REEF Tutorial

- [Installing and Compiling REEF](#install)
- [Running HelloREEF](#running-reef)
    - [Local](#local)
    - [HelloREEFNoClient](#helloREEFNoClient)
    - [YARN](reef-examples.html#yarn)
- [Further Examples](#further-examples)


###<a name="install"></a>Installing and Compiling REEF


####Requirements

- [Java](http://www.oracle.com/technetwork/java/index.html) 7 Development Kit
- [Maven 3](http://maven.apache.org/) or newer. Make sure that `mvn` is in your `PATH`.
- [Protocol Buffers Compiler (protoc) 2.5](http://code.google.com/p/protobuf/) Make sure that protoc is in your PATH. **Note**: You need to install version 2.5. Newer versions won't work.

With these requirements met, the instructions below should work regardless of OS choice and command line interpreter. On Windows, you might find [this](http://cs.markusweimer.com/2013/08/02/how-to-setup-powershell-for-github-maven-and-java-development/) tutorial helpful in setting up PowerShell with Maven, GitHub and Java. You will still have to install the [Protocol Buffers Compiler](https://code.google.com/p/protobuf/), though.

####Cloning the repository
#####Comitters
    $ git clone https://git-wip-us.apache.org/repos/asf/incubator-reef.git

#####Users
    $ git clone git://git.apache.org/incubator-reef.git

####Compiling the code
REEF is built using Maven. Hence, a simple

    $ mvn clean install

should suffice. Note that we have quite a few integration tests in the default build. Hence, you might be better off using

    $ mvn -TC1 -DskipTests clean install

This runs one thread per core (`-TC1`) and skips the tests (`-DskipTests`)

**Note**: You will see many exception printouts during the compilation of REEF with tests. Those are not, in fact, problems with the build: REEF guarantees that exceptions thrown on remote machines get serialized and shipped back to the Driver. We have extensive unit tests for that feature that produce the confusing printouts.

### <a name="running-reef"></a>Running HelloREEF

####Prerequisites

[You have compiled REEF locally](#install).

####Running your first REEF program: Hello, REEF!

The module REEF Examples in the folder `reef-examples` contains several simple programs built on REEF to help you get started with development. As always, the simplest of those is our "Hello World": Hello REEF. Upon launch, it grabs a single Evaluator and submits a single Task to it. That Actvity, fittingly, prints 'Hello REEF!' to stdout. To launch it, navigate to `REEF_HOME` and use the following command:

    java -cp reef-examples/target/reef-examples-{$REEF_VERSION}-incubating-shaded.jar org.apache.reef.examples.hello.HelloREEF

**Note**: *The path divider may be different for different OS (e.g. Windows uses \\ while Linux uses / for dividers) and the version number of your version of REEF must be placed instead of the \* so change the code as needed.*

This invokes the shaded jar within the target directory and launches HelloREEF on the local runtime of REEF. During the run, you will see something similar to this output:

    Powered by
         ___________  ______  ______  _______
        /  ______  / /  ___/ /  ___/ /  ____/
       /     _____/ /  /__  /  /__  /  /___
      /  /\  \     /  ___/ /  ___/ /  ____/
     /  /  \  \   /  /__  /  /__  /  /
    /__/    \__\ /_____/ /_____/ /__/
    
    ...
    INFO: REEF Version: 0.10.0-incubating
    ...
    INFO: The Job HelloREEF is running.
    ...
    INFO: The Job HelloREEF is done.
    ...
    INFO: REEF job completed: COMPLETED

####Where's the output?

The local runtime simulates a cluster of machines: It executes each Evaluator in a separate process on your machine. Hence, the Evaluator that printed "Hello, REEF" is not executed in the same process as the program you launched above. So, how do you get to the output of the Evaluator? The local runtime generates one folder per job it executes in `REEF_LOCAL_RUNTIME`:

    > cd REEF_LOCAL_RUNTIME
    > ls HelloREEF*
    Mode                LastWriteTime     Length Name
    ----                -------------     ------ ----
    d----         1/26/2015  11:21 AM            HelloREEF-1422238904731
    
The job folder names are comprised of the job's name (here, `HelloREEF`) and the time stamp of its submission (here, `1422238904731`). If you submit the same job multiple times, you will get multiple folders here. Let's move on:

    > cd HelloREEF-1422238904731
    > ls
    Mode                LastWriteTime     Length Name
    ----                -------------     ------ ----
    d----         1/26/2015  11:21 AM            driver
    d----         1/26/2015  11:22 AM            Node-1-1422238907421
    
Inside of the job's folder, you will find one folder for the job's Driver (named `driver`) and one per Evaluator. Their name comprises of the virtual node simulated by the local runtime (here, `Node-1`) followed by the time stamp of when this Evaluator was allocated on that node, here `1422238907421`. As the HelloREEF example program only allocated one Evaluator, we only see one of these folders here. Let's peek inside:

    > cd Node-1-1422238907421
    > ls evaluator*
    Mode                LastWriteTime     Length Name
    ----                -------------     ------ ----
    -a---         1/26/2015  11:21 AM       1303 evaluator.stderr
    -a---         1/26/2015  11:21 AM         14 evaluator.stdout    
    
`evaluator.stderr` contains the output on stderr of this Evaluator, which mostly consists of logs helpful in debugging. `evaluator.stdout` contains the output on stdout. And, sure enough, this is where you find the "Hello, REEF!" message.

####<a name="helloREEFNoClient"></a>The difference between HelloREEF and HelloREEFNoClient

The HelloREEF application has multiple versions that all service different needs and one of these applications, `HelloREEFNoClient`, allows the creation of the Driver and Evaluators without the creation of a Client. In many scenarios involving a cluster of machines, one Client will access multiple Drivers so not every Driver needs to create a Client and that is where the `HelloREEFNoClient` application shines. 

Running `HelloREEFNoClient` is nearly identical to running `HelloREEF`:

    java -cp reef-examples/target/reef-examples-{$REEF_VERSION}-incubating-shaded.jar org.apache.reef.examples.hello.HelloREEFNoClient

**Note**: *The path divider may be different for different OS (e.g. Windows uses \\ while Linux uses / for dividers) and the version number of your version of REEF must be placed instead of the \* so change the code as needed.*

and the output should be the same with `evaluator.stdout` containing the "Hello, REEF!" message.

###<a name="further-examples"></a>Further Examples

Further examples of using REEF can be found [here](reef-examples.html).
