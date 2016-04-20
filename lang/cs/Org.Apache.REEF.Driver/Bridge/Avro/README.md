C# Avro Code-Generated Files
============================
The files here are code-generated with modifications. The code
generation is based on the Avro schemas defined in the folder
lang/java/reef-bridge-client/src/avro. Please do not modify them 
unless there is absolutely a reason to!

Instructions On C# Code-Generation
----------------------------------
To code-generate these files, please use instructions on how 
to use Microsoft.Hadoop.Avro.Tools.exe as provided 
[here](https://azure.microsoft.com/en-us/documentation/articles/hdinsight-dotnet-avro-serialization/)
on the files in lang/java/reef-bridge-client/src/avro.
Note that as of the time of writing (11/10/2015), the 
Microsoft Azure HDInsight Avro Library NuGet does not include 
Microsoft.Hadoop.Avro.Tools.exe. To build it directly from source,
please download the source code [here](http://hadoopsdk.codeplex.com/SourceControl/latest) 
and run msbuild. More information on how to build is available 
on the official documentation provided above.