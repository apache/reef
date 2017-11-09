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
==============================

Prerequisites
-------------

  * Windows OS including Windows 8,10,Server 2012 and Server 2016.
  * Same prerequisites as REEF Java ([Building REEF Java](../java/BUILD.md)), which includes:
     * [Apache Maven](https://maven.apache.org/download.cgi) 3.3 or newer
     * Java development kit version 7 or 8 
     * [Protoc version 2.5](https://github.com/google/protobuf/releases/tag/v2.5.0)
  * Set the following environment variables:
     * Set `M2_HOME` environment variable to location of maven installation
     * Set `JAVA_HOME` environment variable to java installation directory
  * Add the following items to the environment variable `PATH`:
     * Add the location of the `protoc.exe` executable in the windows path
     * Add `%JAVA_HOME%/bin` and `%M2_HOME%/bin` to the windows path as well
  * [Visual Studio](http://www.visualstudio.com) 2017 V. 15.4.3 or newer. Most REEF developers use the free Community
    Edition.
  * NuGet 4.0.0 or later. (Included in VS 2017)
  * xunit.runner.console.2.1.0 package (installing it might require manual restore of NuGet packages).


Instructions
------------

***For Building projects:***

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

***For Building DotNet projects:***

We are in the process of converting the C# support to use dotnet core as well as support for Visual Studio 2017. As a result 
there are <projectname>.DotNet.csproj files that sit beside the existing csproj files. To build the dotnet projects, you will need 
the .NET Core SDK installed on your machine as well as Visual Studio 2017. 

**Note:** Building in Visual Studio 2017 is not yet supported. You will need to build at the command line using the .NET Core SDK tools (dotnet). However Visual Studio 2017 does support opening the DotNet solution. .NET Core applications are supported on both Windows and Linux, the projects will only build fully on Windows since they do target the .net45 framework. 

The .NET Core SDK tools are located here:

[https://github.com/dotnet/cli](https://github.com/dotnet/cli)

Scroll down the page to access the latest builds of .NET Core SDK, download the Windows X64 installer. 

At the time of writing the current version used is 2.0.0-preview1-005825. Any version that is equal or newer should also work.

To build from Visual Studio 2017:

This is not currently supported and you will need to build from the command line however VS2017 can be used to view the solution. 

To open the project in VS2017, open the solution `.\lang\cs\Org.Apache.REEF.DotNet.sln`.

To build from the command line, in the `reef\lang\cs` path, type the following:

    dotnet clean Org.Apache.REEF.DotNet.sln
    dotnet restore Org.Apache.REEF.DotNet.sln
    dotnet build Org.Apache.REEF.DotNet.sln

 * `clean` will clean any previously built binary
 * `restore` restores nuget dependencies
 * `build` builds the solution

To build nuget packages, type the following:

 * `dotnet pack Org.Apache.REEF.DotNet.sln`

This will build nuget packages for each project.

Projects will build out to `reef\bin\<AssemblyName>` and will contain all the platforms that the project supports. Each project 
is in various stages as they are transitioning to dotnet core 2.0 - therefore they may only build .net45 and .net46 assemblies.

Continuous Integration
----------------------

We use [AppVeyor](https://www.appveyor.com/) to run continuous integration for REEF .NET code (i.e. build code and run
tests for all pull requests and commits to master branch).

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

