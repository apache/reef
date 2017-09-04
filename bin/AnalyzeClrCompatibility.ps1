# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Input / Output Settings
$baseDir = ".\lang\cs\"
$buildDir="${baseDir}\bin\x64\Debug"
$outputDirectory = "${buildDir}\ClrCompatibility\"
$incompatibilityFile = "${outputDirectory}\incompatibilities.txt"

# Compatibility Analyzer settings
$libraryDotNetTargets = @(".NET Standard, Version=2.0")
$exeDotNetTargets = @(".NET Core + Platform Extensions, Version=1.0", ".NET Framework, Version=4.5.1")

# Set up the directory structure
If (Test-Path $outputDirectory)
{
	Remove-Item $outputDirectory -Force -Recurse
}
New-Item -ItemType Directory -Force -Path $outputDirectory | Out-Null

# Install the API Portability Analyzer if it is not present
$portabilityAnalyzer = "${buildDir}\ApiPort\ApiPort.exe"
If (!(Test-Path $portabilityAnalyzer))
{
	Write-Output "Downloading the DotNet API Portability Analyzer"
	$analyzerUrl="https://github.com/Microsoft/dotnet-apiport/releases/download/v1.4.0.alpha.00175/ApiPort.zip"
	$analyzerZip="${buildDir}\analyzer.zip"
	Invoke-WebRequest $analyzerUrl -OutFile $analyzerZip
	Add-Type -assembly "System.IO.Compression.Filesystem";
	# Now, external libraries don't understand relative paths
	[IO.Compression.Zipfile]::ExtractToDirectory((Get-Item $analyzerZip).FullName,(Get-Item $buildDir).FullName);
	Remove-Item $analyzerZip
}

# Set up the counter
[int] $totalIncompatibilities = 0

# Start a unique list of dependencies
$missingDependencies = @{}

# Get all the folders
Get-ChildItem $baseDir -dir -Filter "Org.Apache.*" |
	ForEach-Object {
		$projectName = $_.Name

		# Verify that the project is part of the .NET solution
		$csProj = "${baseDir}\${projectName}\${projectName}.csproj"
		If (!(Test-Path $csProj))
		{
			Write-Output "Skipping ${projectName}: No csproj"
			return
		}

		# Determine the output type for the project
		$outputType = $(Select-String -Path $csProj -Pattern "<OutputType>").Line -replace '^\s+<OutputType>(.+?)</OutputType>\s*$','$1'

		# Get the portability settings for this project
		$objectSuffix = ""
		$dotNetTargets = @()
		If ($outputType -eq "Library")
		{
			$objectSuffix = "dll"
			$dotNetTargets = $libraryDotNetTargets
		}
		ElseIf ($outputType -eq "Exe")
		{
			$objectSuffix = "exe"
			$dotNetTargets = $exeDotNetTargets
		}
		Else
		{
			Write-Output "Skipping ${projectName}: OutputType is not 'Library','Exe'"
			return
		}

		# Define the object we want to explore
		$objectPath = "${buildDir}\${projectName}\${projectName}.${objectSuffix}"
		If(!(Test-Path $objectPath)) {
			Write-Output "Warning: Could not find build object: ${objectPath}; Skipping ${projectName}"
			return
		}

		# Announce our intentions (there may be many slow loops)
		Write-Output "Analyzing $projectName - $outputType"

		# Define the portability command
		$outputFileName = "${outputDirectory}\${projectName}.${objectSuffix}.json"
		$analyzeParams = "analyze", "-r", "Json", "-f", $objectPath, "-o", $outputFileName
		ForEach ($target in $dotNetTargets)
		{
			$analyzeParams += "-t", $target
		}

		# Run the command
		& $portabilityAnalyzer $analyzeParams | Out-Null

		# Read in the resulting object
		$compliance = Get-Content -Raw -Path $outputFileName | ConvertFrom-Json
		
		# Count the incompatibilities
		$incompatibilities = $compliance.MissingDependencies.Count
		$totalIncompatibilities += $incompatibilities
		Write-Output "`tFound ${incompatibilities} incompatibilities"

		# Keep a unique list of the incompatible libraries
		ForEach ($item in $compliance.MissingDependencies)
		{
			$assembly = $item.DefinedInAssemblyIdentity.split(',')[0]
			$missingDependencies[$assembly] = 1
		}
	}

# Now report the status
Write-Output "Found ${totalIncompatibilities} total incompatibilities"
Write-Output "Missing Dependencies:"
$dependencyString = "`t" + ($missingDependencies.Keys -join "`n`t")
Write-Output "${dependencyString}"

# And checkpoint it to a file
$totalIncompatibilities | Out-File $incompatibilityFile
