# C# and Avro Code Generation

Currently we use Microsoft Avro for C# and Apache Avro for Java.
Code generation on the java side is handled by the Maven Avro plugin
at compile time. On the C# side a powershell script is used to manually
generate the sources: Generate-CSFromAvro.ps1.

## Prerequisites
* Add the `$REEFSourcePath\lang\cs\packages\Microsoft.Core.Avro{Version}\lib` directory to your path.
* Install the Visual Studio PowerShell Tools extension.

## Code Generation
Regenerating the C# files is only necessary if you change one of the `.avsc` files in the `lang\cs\common\bridge\avro` directory.
* Right click on Generate-CSFromAvro.ps1 script in the Visual Studio solution explorer and select *Execute as Script*.