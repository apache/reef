<#
.SYNOPSIS
Helper unctions to run REEF applications 
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

function GetYarnClasspath {
  <#
  .SYNOPSIS
  Returns the classpath setup by YARN.
  .DESCRIPTION
  A simple wrapper around "yarn classpath" that cleans up the classpath returned and formats it into a list.
  #>
  (Invoke-Expression -Command "$env:HADOOP_HOME\bin\yarn.cmd classpath").split(";") | Where-Object {$_ -ne ""} | Unique
}


function SubmitYarnApplication {
  <#
  .SYNOPSIS
  Runs a given main class from a given JAR file on a YARN cluster.
  .DESCRIPTION
  Runs a given main class from a given JAR file on a YARN cluster. This assumes
  that $JAVA_HOME is set and that yarn is on the $PATH.
  #>
  param
  (
    # the jar file(s) with the project inside
    [Parameter(Mandatory=$True, HelpMessage="The JAR file(s) to launch.", Position=1)]
    [string[]]$Jars,

    # the main class to run
    [Parameter(Mandatory=$True, HelpMessage="The main class to launch.", Position=2)]
    [string]$Class,

    [Parameter(HelpMessage="Turn on detailed logging.")]
    [switch]$VerboseLog,

    [Parameter(Mandatory=$False, HelpMessage="Options to be passed to java")]
    [string]$JavaOptions,

    [Parameter(ValueFromRemainingArguments=$True)]
    $Arguments
  )

  $Jars | %{
    # Check whether the file exists
    if (!(Test-Path $_)) {
      Write-Host "Error: JAR file doesn't exist: " $_
      exit
    }
  }

  # Assemble the classpath for the job
  if ($env:HADOOP_HOME) {
    $CLASSPATH = (GetYarnClasspath) + $Jars -join ";"
  } else {
    $CLASSPATH = $Jars -join ";"
  }

  # the logging command
  if ($VerboseLog) {
    $LogParams = "`"-Djava.util.logging.config.class=org.apache.reef.util.logging.Config`""
  }

  # Assemble the command to run
  # Note: We need to put the classpath within "", as it contains ";"
  $command = "& `"$env:JAVA_HOME\bin\java.exe`" $JavaOptions -cp `"$CLASSPATH`" $LogParams $Class $Arguments"

  if ($VerboseLog) {
    echo $command
  }

  Invoke-Expression -Command $command
}
