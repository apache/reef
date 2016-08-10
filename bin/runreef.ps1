<#
.SYNOPSIS
Runs a given main class from a given JAR file on a YARN cluster.
.DESCRIPTION
Runs a given main class from a given JAR file on a YARN cluster. This assumes
that JAVA_HOME and HADOOP_HOME are set.
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
param
  (
    # the jar file(s) with the project inside
    [Parameter(Mandatory=$False, HelpMessage="The JAR file(s) to launch.", Position=1)]
    [string]$Jars,

    # the main class to run
    [Parameter(Mandatory=$False, HelpMessage="The main class to launch.", Position=2)]
    [string]$Class,

    [Parameter(HelpMessage="Turn on detailed logging.")]
    [switch]$VerboseLog,

    [Parameter(Mandatory=$False, HelpMessage="Options to be passed to java")]
    [string]$JavaOptions,

    [Parameter(ValueFromRemainingArguments=$True)]
    $Arguments
  )

Import-Module ((Split-Path -Parent $MyInvocation.MyCommand.Definition) + "\runreef.psm1")

if ((Split-Path -Leaf $MyInvocation.MyCommand.Definition).Equals("runreef.ps1")) {
  SubmitYarnApplication `
    -Jars ($Jars -split ';') `
    -Class $Class `
    -JavaOptions $JavaOptions `
    -VerboseLog:$VerboseLog `
    -Arguments $Arguments
}
