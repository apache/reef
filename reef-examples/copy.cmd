@REM
@REM Copyright (C) 2013 Microsoft Corporation
@REM
@REM Licensed under the Apache License, Version 2.0 (the "License");
@REM you may not use this file except in compliance with the License.
@REM You may obtain a copy of the License at
@REM
@REM         http://www.apache.org/licenses/LICENSE-2.0
@REM
@REM Unless required by applicable law or agreed to in writing, software
@REM distributed under the License is distributed on an "AS IS" BASIS,
@REM WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@REM See the License for the specific language governing permissions and
@REM limitations under the License.
@REM

copy /y %enlistment_root%\bin\x64\Debug\Incubation\Evaluator\*.* %REEF_HOME%\reef-examples\dotnet  
copy /y %enlistment_root%\bin\x64\Debug\Incubation\Streaming\StreamInsightActivity\Microsoft.DataPipeline.ComputeService.StreamInsightActivity.* %REEF_HOME%\reef-examples\dotnet
copy /y %enlistment_root%\bin\x64\Debug\Incubation\Streaming\StreamInsightTask\Microsoft.DataPipeline.ComputeService.StreamInsightTask.* %REEF_HOME%\reef-examples\dotnet
copy /y %enlistment_root%\bin\x64\Debug\Incubation\Streaming\StreamInsightTask\Microsoft.DataPipeline.ComputeService.BlobSIAdapter.* %REEF_HOME%\reef-examples\dotnet
copy /y %enlistment_root%\bin\x64\Debug\Incubation\Streaming\StreamInsightTask\Microsoft.DataPipeline.ComputeService.ServiceBusAdapter.* %REEF_HOME%\reef-examples\dotnet
copy /y %enlistment_root%\bin\x64\Debug\Incubation\Streaming\StreamInsightTask\Microsoft.DataPipeline.ComputeService.Util.* %REEF_HOME%\reef-examples\dotnet
copy /y %enlistment_root%\bin\x64\Debug\Incubation\Streaming\StreamInsightTask\Microsoft.DataPipeline.ComputeService.SqlAzureSIAdapter.* %REEF_HOME%\reef-examples\dotnet
copy /y %enlistment_root%\bin\x64\Debug\Incubation\Streaming\StreamInsightTask\Microsoft.DataPipeline.ComputeService.PayloadTypes.* %REEF_HOME%\reef-examples\dotnet
copy /y %enlistment_root%\bin\x64\Debug\Incubation\Streaming\StreamInsightTask\Microsoft.WindowsAzure.Storage.* %REEF_HOME%\reef-examples\dotnet
copy /y %enlistment_root%\bin\x64\Debug\Incubation\Streaming\StreamInsightTask\Microsoft.Practices.EnterpriseLibrary.WindowsAzure.TransientFaultHandling.* %REEF_HOME%\reef-examples\dotnet
copy /y %enlistment_root%\bin\x64\Debug\Incubation\Streaming\StreamInsightTask\Microsoft.Practices.EnterpriseLibrary.Common.* %REEF_HOME%\reef-examples\dotnet
copy /y %enlistment_root%\bin\x64\Debug\Incubation\Streaming\StreamInsightTask\Microsoft.Practices.TransientFaultHandling.Core.* %REEF_HOME%\reef-examples\dotnet
copy /y %enlistment_root%\bin\x64\Debug\Incubation\Streaming\StreamInsightTask\System.Reactive.* %REEF_HOME%\reef-examples\dotnet
copy /y %enlistment_root%\bin\x64\Debug\Incubation\Streaming\StreamInsightTask\Microsoft.ComplexEventProcessing.* %REEF_HOME%\reef-examples\dotnet

copy /y %enlistment_root%\bin\x64\Debug\Incubation\ITask\Microsoft.Reef.Tasks.ITask.* %REEF_HOME%\reef-examples\dotnet
copy /y %enlistment_root%\bin\x64\Debug\Incubation\StreamingTask\Microsoft.Reef.Tasks.StreamingTask.* %REEF_HOME%\reef-examples\dotnet
copy /y %enlistment_root%\bin\x64\Debug\Incubation\ClassHierarchyBuilder\task.bin %REEF_HOME%\reef-examples\dotnet




