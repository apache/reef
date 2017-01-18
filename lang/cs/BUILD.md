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

Building and Testing REEF .NET
==================

Prerequisites
-------------

  * Windows OS.
  * [Building REEF Java](../java/BUILD.md).
  * [Visual Studio](http://www.visualstudio.com) 2015 (preferred) or 2013. Most REEF developers use the free Community Edition.
  * NuGet 2.8.6 or later.
  * xunit.runner.console.2.1.0 package (installing it might require manual restore of NuGet packages).


Instructions
------------

To build and run tests in Visual Studio on local machine:

1. Open solution `.\lang\cs\Org.Apache.REEF.sln`
2. Build solution
3. Open Test Explorer and add search filter `-Trait:Environment` to filter out tests which are supposed to run on Yarn.
   If not filtered out, these tests will fail.
4. Use "Run All" to run all tests

To build REEF.NET from command line, execute

    msbuild .\lang\cs\Org.Apache.REEF.sln

To run .NET tests on local machine from command line, execute

    msbuild .\lang\cs\TestRunner.proj

`TestRunner.proj` already has a filter set up to exclude Yarn tests.

Continuous Integration
------------

We use [AppVeyor](https://www.appveyor.com/) to run continuous integration for REEF .NET code (i.e. build code and run tests
for all pull requests and commits to master branch).

It can be convenient to set up AppVeyor for your fork of REEF repository, for example, to reproduce a test failure which
can't be reproduced locally or to get AppVeyor test run results earlier than the official ones.

1. Log in to [AppVeyor](https://ci.appveyor.com/) using your GitHub credentials.
2. Go to [project creation](https://ci.appveyor.com/projects/new) and select `reef` repository.
3. Fine-tune configuration as required at Settings tab of repository; you can enable building pushes to your repository
   when you're investigating something and disable them when you don't need them.
4. Edit [AppVeyor configuration file](../../appveyor.yml) as required (by default you'll use the same configuration as REEF build).

To isolate a specific test or group of tests, you can modify `test_script` section of `appveyor.yml`.
For example, to run only one test `Org.Apache.REEF.Tests.Functional.IMRU.IMRUCloseTaskTest.TestTaskCloseOnLocalRuntime`
modify `test_script` section as follows:

    test_script:
      - cmd: cd .\lang\cs
      - cmd: .\.nuget\nuget.exe restore .\.nuget\packages.config -o .\packages
      - cmd: .\packages\xunit.runner.console.2.1.0\tools\xunit.console.exe .\bin\x64\Debug\Org.Apache.REEF.Tests\Org.Apache.REEF.Tests.dll -method Org.Apache.REEF.Tests.Functional.IMRU.IMRUCloseTaskTest.TestTaskCloseOnLocalRuntime

`nuget restore` is necessary to install `xunit.runner.console` package, which by default is installed in `TestRunner.proj`.

Instructions on Building and Testing REEF .NET from Scratch
------------

Here is a step-by-step guide, if the instructions above didn't work and/or you prefer to build REEF .NET from Windows Command Prompt/PowerShell.

1. Install Java Development Kit 7 or 8. Set `JAVA_HOME` as an Environment Variable
    * Go [here](http://www.oracle.com/technetwork/java/javase/downloads) to download and install appropriate JDK
    * Go to System Properties -> Environment Variables -> System Variables -> New...
    * Create a variable called `JAVA_HOME` and set the value to be your jdk installation dir, like C:\Program Files\Java\jdk1.8.0_91
    * Update the Environment Variable `Path` by adding `%JAVA_HOME%\bin`

2. Install Maven 3.3.9. Set `M2_HOME` and `M2` as Environment Variables
    * Go [here](https://archive.apache.org/dist/maven/maven-3/3.3.9/binaries/) to download apache-maven-3.3.9-bin.zip
    * Unzip it and place it to your desired location
    * Create an Environment Variable called `M2_HOME` and set the value to be your unzip location, like C:\Program Files\Apache\apache-maven-3.3.9
    * Create another variable called `M2` and set the value to be `%M2_HOME%\bin`
    * Update the Environment Variable `Path` by adding `%M2%` and `%M2_HOME%`

3. Install Protocol Buffer 2.5. Add its path as an Environment Variable
    * Go [here](https://github.com/google/protobuf/releases/tag/v2.5.0) to download protoc-2.5.0-win32.zip
    * Unzip it and place it to your desired location. Make sure that protoc.exe is in that folder
    * Update the Environment Variable `Path` by adding the unzip location, like C:\protobuf-2.5.0\src\protoc-2.5.0-win32

4. Git clone the repo to your local machine: `git clone git@github.com:apache/reef.git`

5. To build REEF.NET from command line, execute: `msbuild .\lang\cs\Org.Apache.REEF.sln`
