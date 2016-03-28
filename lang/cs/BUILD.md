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

To build and run tests in Visual Studio, open `lang\cs\Org.Apache.REEF.sln`, build entire solution and run all tests in Test Explore.

To build REEF.NET from command line, execute

    msbuild .\lang\cs\Org.Apache.REEF.sln

To run .NET tests from command line, execute

    msbuild .\lang\cs\TestRunner.proj
