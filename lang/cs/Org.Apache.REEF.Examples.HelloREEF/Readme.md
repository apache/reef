# HelloREEF
This project contains a simple example of a REEF Program. It contains the following classes:

  * `HelloREEF`: This is the program that submits the driver to the local runtime.
  * `HelloDriver`: The Driver requests a single Evaluator and submits the `HelloTask` to it.
  * `HelloTask`: This Task prints a greeting to STDOUT of the Evaluator.

## Running it
Just run the main class, `HelloREEF`, followed by the runtime you want, e.g. `local`.

## Using as a Standalone Application
To use HelloREEF as a standalone application, the only changes necessary are installing dependencies and adding a build step to the .csproj file.

Directions
1. Copy all the \*.cs files to a new directory
2. In Visual Studio, create a new project from these files
3. Change the namespaces to `HelloREEF`
4. Change the .NET framework to `4.5.1`
5. Change the target platform to `x64`
6. Use the NuGet to install all dependencies necessary to build the project
7. Add the following `References` to the .csproj file
```
<Reference Include="Org.Apache.REEF.Evaluator, Version=0.16.1.0, Culture=neutral, PublicKeyToken=c27bf5b2e9a7ddb9, processorArchitecture=AMD64\">
  <HintPath>packages/Org.Apache.REEF.Evaluator.0.16.1/tools/Org.Apache.REEF.Evaluator.exe</HintPath>
</Reference>
```
8. Build the project one last time

Now HelloREEF can be used as the first step of writing a new application with Apache REEF.