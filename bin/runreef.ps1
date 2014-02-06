<#
 .SYNOPSIS
 Runs a given main class from a given JAR file on a YARN cluster.
 .DESCRIPTION
 Runs a given main class from a given JAR file on a YARN cluster. This assumes 
 that $JAVA_HOME is set and that yarn is on the $PATH.
 #>

<# 
#  Copyright (C) 2014 Microsoft Corporation
# 
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
# 
#          http://www.apache.org/licenses/LICENSE-2.0
# 
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
#>

[CmdletBinding()]
  param
  (
    # the jar file with the project inside
    [Parameter(Mandatory=$True, HelpMessage="The JAR file to launch.", Position=1)]
    [string]$Jar,
    # the main class to run
    [Parameter(Mandatory=$True, HelpMessage="The main class to launch.", Position=2)]
    [string]$Class,
    [Parameter(Mandatory=$False, HelpMessage="Options to be passed to java")]
    [string]$JavaOptions,

    [Parameter(ValueFromRemainingArguments=$True)]
    $Arguments
  )

function Get-YARN-Classpath{
 <#
 .SYNOPSIS
 Returns the classpath setup by YARN.
 .DESCRIPTION
 A simple wrapper around "yarn classpath" that cleans up the classpath returned and formats it into a list.
 #>
 (yarn classpath).split(";") | where {$_ -ne ""} | Unique
}

function Submit-YARN-Application{
<#
 .SYNOPSIS
 Runs a given main class from a given JAR file on a YARN cluster.
 .DESCRIPTION
 Runs a given main class from a given JAR file on a YARN cluster. This assumes 
 that $JAVA_HOME is set and that yarn is on the $PATH.
 #>
[CmdletBinding()]
  param
  (
    # the jar file with the project inside
    [Parameter(Mandatory=$True, HelpMessage="The JAR file to launch.", Position=1)]
    [string]$Jar,
    # the main class to run
    [Parameter(Mandatory=$True, HelpMessage="The main class to launch.", Position=2)]
    [string]$Class,
    [Parameter(Mandatory=$False, HelpMessage="Options to be passed to java")]
    [string]$JavaOptions,
    [Parameter(ValueFromRemainingArguments=$True)]
    $Arguments
  )
  # check whether yarn is in the PATH
  if (!(Get-Command yarn -errorAction SilentlyContinue))
  {
   Write-Host "Error: yarn is not on the PATH."
   exit 
  }
  
  # Assemble the classpath for the job
  $CLASSPATH = ((Get-YARN-Classpath) + $Jar) -join ";"

  # Assemble the command to run
  # Note: We need to put the classpath within "", as it contains ";"
  $command = "$env:JAVA_HOME\bin\java.exe $JavaOptions -cp `"$CLASSPATH`"  $Class $Arguments"
  Invoke-Expression -Command $command
}

Submit-YARN-Application -Jar $Jar -Class $Class -JavaOptions $JavaOptions -Arguments $Arguments
