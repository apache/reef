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

Param(
    [Parameter(Mandatory=$true)]
    [string]$SolutionDir,

    [switch]$Snapshot,

    [int]$SnapshotNumber
)

Function Get-Nuspec-Version {
    <#
    .DESCRIPTION
    Extracts the NuGet version number from the pom.xml file in the source directory.
    #>

    $pomPath = "$SolutionDir\pom.xml"
    $pom = [xml] (Get-Content $pomPath)
    $version = $pom.project.parent.version -replace '-incubating-SNAPSHOT',''
    return $version
}

Function Prep-Nuspec-Files {
    <#
    .DESCRIPTION
    Creates a directory for the finalized nuspec files to live.  Next,
    the temporary nuspec files in each source directory will get copied
    to the new nuspec directory.
    #>

    $nuspecDir = "$SolutionDir\.nuget\nuspec"

    # Delete the directory if it already exists
    if (Test-Path $nuspecDir) {
        rmdir -Force -Recurse $nuspecDir
    }

    # Create directory for finalized nuspec files to live
    mkdir -Force $nuspecDir

    # Copy over temporary nuspec files into new nuspec directory
    $tempNuspecFiles = Get-ChildItem $SolutionDir\**\*.nuspec
    foreach ($tempNuspecFile in $tempNuspecFiles) {
        Copy-Item $tempNuspecFile.FullName $nuspecDir
    }
}

Function Finalize-Nuspec-Version {
    <#
    .DESCRIPTION
    Replaces the $version$ token in each nuspec file with the actual version string.
    #>

    param([string]$version)

    if ($Snapshot) {
        $fullVersion = "$version-SNAPSHOT-$SnapshotNumber"
    } 
    else {
        $fullVersion = $version
    }

    $nuspecDir = "$SolutionDir\.nuget\nuspec"
    Write-Output $nuspecDir
    $nuspecFiles = Get-ChildItem $nuspecDir
    
    # Replace the $version$ token with the specified version in each nuspec file
    foreach ($nuspec in $nuspecFiles) {
        $finalizedNuspec = Get-Content $nuspec.FullName | foreach { $_ -replace '\$version\$',"$fullVersion" }
        Set-Content -Path $nuspec.FullName -Value $finalizedNuspec
    }
}

Prep-Nuspec-Files
$version = Get-Nuspec-Version
Finalize-Nuspec-Version($version)
Exit $LASTEXITCODE 
