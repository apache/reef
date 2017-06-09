#
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
#

$ReefRoot = [Environment]::GetEnvironmentVariable("REEFSourcePath")
if ($ReefRoot -eq $null)
{
    Write-Host "REEFSourcePath must be set" -ForegroundColor Red
    exit
}

if ((Get-Command "Microsoft.Avro.Tools.exe" -ErrorAction SilentlyContinue) -eq $null)
{
    Write-Host "Microsoft.Avro.Tools.exe must be in path" -ForegroundColor Red
    exit
}

$AvroSources = @(
    "Header",
    "Acknowledgement"
    "Protocol",
    "SystemOnStart"
)

$SrcPath = "$ReefRoot\lang\common\bridge\avro\"
$DstPath = "$ReefRoot\lang\cs\Org.Apache.REEF.Bridge.CLR\Message\"
$Copyright = "Copyright.txt"

foreach ($SourceFile in $AvroSources)
{
    Remove-Item "$DstPath$SourceFile.cs" -ErrorAction SilentlyContinue
    Remove-Item "$DstPath$SourceFile.tmp" -ErrorAction SilentlyContinue

    Write-Host "Processing $SourceFile" -ForegroundColor Green
    & "Microsoft.Avro.Tools.exe" CodeGen /I:"$SrcPath$SourceFile.avsc" /O:$DstPath
    Rename-Item "$DstPath$SourceFile.cs" "$DstPath$SourceFile.tmp" -Force
    Copy-Item $SrcPath$Copyright -Destination "$DstPath$SourceFile.cs"
    Add-Content "$DstPath$SourceFile.cs" -Value (Get-Content "$DstPath$SourceFile.tmp")
    Remove-Item "$DstPath$SourceFile.tmp"
}
