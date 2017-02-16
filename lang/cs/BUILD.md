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

  * Windows OS including Windows 8,10,Server 2012 and Server 2016.
  * Same prerequisites as REEF Java ([Building REEF Java](../java/BUILD.md)), which includes:
     * [Apache Maven] (https://maven.apache.org/download.cgi) 3.3 or newer
     * Java development kit version 7 or 8 
     * Protoc version 2.5 (https://github.com/google/protobuf/releases/tag/v2.5.0)
  * Set the following environment variables:
     * Set M2_HOME environment variable to location of maven installation
     * Set JAVA_HOME environment variable to java installation directory
  * Add the following items to the environment variable path:
     * Add the location of the protoc executable in the windows path
     * Add %JAVA_HOME%/bin and %M2_HOME%/bin to the windows path as well
  * [Visual Studio](http://www.visualstudio.com) 2015 (preferred) or 2013. Most REEF developers use the free Community Edition.
  * NuGet 2.8.6 or later. (Included in VS 2015)
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

