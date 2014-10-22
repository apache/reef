<#
.SYNOPSIS
Run given unit tests on YARN.
.DESCRIPTION
Take class names of JUnit tests to run and start them on the YARN cluster.
This assumes that JAVA_HOME and HADOOP_HOME are set.
#>

<#
Copyright (C) 2014 Microsoft Corporation
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
#>
param
  (
    # JAR file(s) to add to the classpath.
    [Parameter(Mandatory=$True, HelpMessage="JAR file(s) to use")]
    [string]$Jars,

    [Parameter(Mandatory=$False, HelpMessage="Options to be passed to java")]
    [string]$JavaOptions,

    [Parameter(HelpMessage="Turn on detailed logging.")]
    [switch]$VerboseLog,

    [Parameter(ValueFromRemainingArguments=$True, HelpMessage="Class names of unit tests to run")]
    $Tests="com.microsoft.reef.tests.AllTestsSuite"
  )

Import-Module ((Split-Path -Parent -Resolve $MyInvocation.MyCommand.Definition) + "\runreef.psm1")

if ((Split-Path -Leaf $MyInvocation.MyCommand.Definition).Equals("runtests.ps1")) {
  $env:REEF_TEST_YARN = "true"
  Submit-YARN-Application -Jars ($Jars -split ';') -Class org.junit.runner.JUnitCore -JavaOptions $JavaOptions -VerboseLog:$VerboseLog -Arguments $Tests
}
