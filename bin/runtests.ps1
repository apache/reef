<#
.SYNOPSIS
Run given unit tests on YARN.
.DESCRIPTION
Take class names of JUnit tests to run and start them on the YARN cluster.
This assumes that JAVA_HOME and HADOOP_HOME are set.
#>

<#
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
#>
[CmdletBinding(PositionalBinding=$False)]
param
  (
    # JAR file(s) to add to the classpath.
    [Parameter(Mandatory=$True, HelpMessage="Semicolon-separated list of JARs to use")]
    [string]$Jars,

    [Parameter(Mandatory=$False, HelpMessage="Options to be passed to Java")]
    [string]$JavaOptions,

    [Parameter(HelpMessage="Run unit tests on YARN.")]
    [switch]$Yarn,

    [Parameter(HelpMessage="Turn on detailed logging.")]
    [switch]$VerboseLog,

    [Parameter(ValueFromRemainingArguments=$True, HelpMessage="Class names of unit tests to run")]
    $Tests="org.apache.reef.tests.AllTestsSuite"
  )

Import-Module ((Split-Path -Parent -Resolve $MyInvocation.MyCommand.Definition) + "\runreef.psm1")

if ((Split-Path -Leaf $MyInvocation.MyCommand.Definition).Equals("runtests.ps1")) {

  $env:REEF_TEST_YARN = $Yarn.IsPresent

  SubmitYarnApplication `
    -Jars ($Jars -split ';') `
    -Class org.junit.runner.JUnitCore `
    -JavaOptions $JavaOptions `
    -VerboseLog:$VerboseLog.IsPresent `
    -Arguments $Tests
}
