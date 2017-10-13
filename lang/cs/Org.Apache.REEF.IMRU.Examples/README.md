# Running IMRU Examples

The easiest way to run IMRU examples is from command line.
Build C# code using your preferred method.

## Running on local runtime

Navigate to `<reef>\bin\x64\$(Configuration)\Org.Apache.REEF.IMRU.Examples` (`$(Configuration)` is `Debug` or `Release` based on configuration for which you're building). 

Run `.\Org.Apache.REEF.IMRU.Examples.exe` with first parameter set to `false`.

## Running on Apache Hadoop YARN

Log in to a machine in YARN cluster from which you'll run the job.

Copy the contents of `<reef>\bin\x64\$(Configuration)\Org.Apache.REEF.IMRU.Examples` to this machine.

Run `.\Org.Apache.REEF.IMRU.Examples.exe` with first parameter set to `true`.

## Command line parameters

Command line parameters are parsed based on their position in the list of parameters, so if you need to pass a non-default value for a parameter, you have to pass explicit values for all preceding parameters.

1. run on YARN (default `false`): if `true`, run the test on YARN, otherwise run the test on local runtime.
2. number of nodes (default `2`): number of nodes on which IMRU job will run. This number includes the master node, i.e. the number of mappers will be one less than the value you pass.
3. start port (default `8900`): the start of port range.
4. port range (default `1000`): the number of ports in port range. Many clusters have firewall configurations which only allow processes to open ports in specific ranges. These are specified here as a start and the length of the port range. REEF will pick a random available port from that range for its communications.
5. test name (default `MapperCount`): the name of the test to run. Can take one of the three values (case-insensitive): 
* `MapperCount` (simple job which counts the number of mappers it started), 
* `BroadcastAndReduce` (performs broadcast and reduce on a dataset) or
* `BroadcastAndReduceFT` (same with fault tolerance).

`MapperCount` job has one extra parameter: the name of the remote file to write output to (default value is empty, so the result is written only to local file).

`BroadcastAndReduce` job performs a basic IMRU job. `Update` sends the current iteration count as a n-dimensional array of integers. `Map` checks that the its input is correct and merely returns it. `Reduce` sums up the arrays. Both plain and fault-tolerant version of `BroadcastAndReduce` accept the following parameters:

6. `dimensions` (default: `10`): The number of dimensions in the `int[]` array being bounced back and forth between Mapper and Updater.
7. `chunkSize` (default: `2`): The number of dimensions to be processed at once in the Reducers. Note: IMRU performs pipelining. This parameter allows you to play with the effects of that.
8. `mapperMemory` (default: `512`): Amount of memory to be allocated for each map task, in MB.
9. `updateTaskMemory` (default: `512`): Amount of memory to be allocated to the update task, in MB.
10. `iterations` (default: `100`): The number of iterations this example should run for. Set to `1` to measure the overhead of starting the job.

`BroadcastAndReduceFT` job runs the same workload as `BroadcastAndReduce`, but with some of the nodes forced to fail. This job is used to test fault-tolerance of IMRU scenario. It accepts two extra parameters:

11. `maxRetryNumberInRecovery` (default 10): The number of times IMRU job will try to recover from failures of some of the mappers.
12. `totalNumberOfForcedFailures` (default 2): The number of retries in which forced mapper failures will be introduced. For jobs running on local runtime, this is the main source of failures. For jobs running on YARN, environment will typically provide sufficient number of natural failures to not need artificial ones.
