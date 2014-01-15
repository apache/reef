copy /y %enlistment_root%\bin\x64\Debug\Incubation\Evaluator\*.* %REEF_HOME%\reef-examples\dotnet  
copy /y %enlistment_root%\bin\x64\Debug\Incubation\Streaming\StreamInsightActivity\Microsoft.DataPipeline.ComputeService.StreamInsightActivity.* %REEF_HOME%\reef-examples\dotnet
copy /y %enlistment_root%\bin\x64\Debug\Incubation\Streaming\StreamInsightActivity\Microsoft.DataPipeline.ComputeService.StreamInsightTask.* %REEF_HOME%\reef-examples\dotnet
copy /y %enlistment_root%\bin\x64\Debug\Incubation\Streaming\StreamInsightActivity\Microsoft.DataPipeline.ComputeService.BlobSIAdapter.* %REEF_HOME%\reef-examples\dotnet
copy /y %enlistment_root%\bin\x64\Debug\Incubation\Streaming\StreamInsightActivity\Microsoft.DataPipeline.ComputeService.ServiceBusAdapter.* %REEF_HOME%\reef-examples\dotnet
copy /y %enlistment_root%\bin\x64\Debug\Incubation\Streaming\StreamInsightActivity\Microsoft.DataPipeline.ComputeService.Util.* %REEF_HOME%\reef-examples\dotnet
copy /y %enlistment_root%\bin\x64\Debug\Incubation\Streaming\StreamInsightActivity\Microsoft.DataPipeline.ComputeService.SqlAzureSIAdapter.* %REEF_HOME%\reef-examples\dotnet
copy /y %enlistment_root%\bin\x64\Debug\Incubation\Streaming\StreamInsightActivity\Microsoft.DataPipeline.ComputeService.PayloadTypes.* %REEF_HOME%\reef-examples\dotnet
copy /y %enlistment_root%\bin\x64\Debug\Incubation\Streaming\StreamInsightActivity\Microsoft.WindowsAzure.Storage.* %REEF_HOME%\reef-examples\dotnet
copy /y %enlistment_root%\bin\x64\Debug\Incubation\Streaming\StreamInsightActivity\Microsoft.Practices.EnterpriseLibrary.WindowsAzure.TransientFaultHandling.* %REEF_HOME%\reef-examples\dotnet
copy /y %enlistment_root%\bin\x64\Debug\Incubation\Streaming\StreamInsightActivity\Microsoft.Practices.EnterpriseLibrary.Common.* %REEF_HOME%\reef-examples\dotnet
copy /y %enlistment_root%\bin\x64\Debug\Incubation\Streaming\StreamInsightActivity\Microsoft.Practices.TransientFaultHandling.Core.* %REEF_HOME%\reef-examples\dotnet
copy /z /y %REEF_HOME%\..\TANG\tang\src\test\cs\TangTest\bin\x64\Debug\activity.bin  %REEF_HOME%\reef-examples\dotnet
copy /z /y %REEF_HOME%\..\TANG\tang\src\test\cs\TangTest\bin\x64\Debug\com.microsoft.reef.activity.*  %REEF_HOME%\reef-examples\dotnet
copy /z /y %REEF_HOME%\..\TANG\tang\src\test\cs\TangTest\bin\x64\Debug\com.microsoft.reef.activityInterface.*  %REEF_HOME%\reef-examples\dotnet




