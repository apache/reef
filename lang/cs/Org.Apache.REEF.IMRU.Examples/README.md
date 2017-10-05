# Running IMRU Examples

The easiest way to run IMRU examples is from command line.
Build C# code using your preferred method.

## Running on local runtime

Navigate to `<reef>\bin\x64\Debug\Org.Apache.REEF.IMRU.Examples`. 

Run `.\Org.Apache.REEF.IMRU.Examples.exe` with first parameter set to `false`.

## Running on Yarn

Log in to a machine in Yarn cluster from which you'll run the job.

Copy the contents of `<reef>\bin\x64\Debug\Org.Apache.REEF.IMRU.Examples` to this machine.

Run `.\Org.Apache.REEF.IMRU.Examples.exe` with first parameter set to `true`.

## Command line parameters

Command line parameters are parsed based on their position in the list of parameters, so if you need to pass a non-default value for a parameter, you have to pass explicit values for all preceding parameters.

1. run on yarn (default `false`): if `true`, run the test on Yarn, otherwise run the test on local runtime.
2. number of nodes (default `2`): number of nodes on which IMRU job will run. This number includes the master node, i.e. the number of mappers will be one less than the value you pass.
3. start port (default `8900`): the start of port range.
4. port range (default `1000`): the number of ports in port range.
5. test name (default `MapperCount`): the name of the test to run. Can take one of the three values (case-insensitive): 
* `MapperCount` (simple job which counts the number of mappers it started), 
* `BroadcastAndReduce` (performs broadcast and reduce on a dataset) or
* `BroadcastAndReduceFT` (same with fault tolerance).

MapperCount job has one extra parameter: the name of the remote file to write output to (default value is empty, so the result is written only to local file).

BroadcastAndReduce job has several extra parameters.
