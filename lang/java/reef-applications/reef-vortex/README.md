Vortex
========================
Vortex is a runtime for low-priority jobs to effectively cope 
with non-cooperative(thus low-latency) preemptions with the following goals:

* Enable high-priority jobs such as latency-sensitive online serving jobs to reclaim resources quickly
* Enable low-priority jobs such as offline batch data analytics jobs to efficiently utilize volatile resources

Vortex is currently under [active development](https://issues.apache.org/jira/browse/REEF-364)

Key Components
========================
* ThreadPool API
    * Users can submit custom functions and inputs to Vortex and retrieve results
    * This API hides the details of distributed execution on unreliable resources(e.g. handling of preemption)
* Tasklet
    * User-submitted function and input are translated into Tasklet(s) in VortexMaster
    * A Tasklet is then scheduled to and executed by a VortexWorker
* Queue
    * VortexMaster Queue(in REEF Driver): Keeps track of Tasklets waiting to be scheduled/enqueued to a VortexWorker Queue
    * VortexWorker Queue(in REEF Evaluator): Keeps track of Tasklets waiting to be executed

Example: Vector Calculation on Vortex
====================
```java
/**
 * User's main function.
 */
public final class AddOne {
  private AddOne() {
  }

  public static void main(final String[] args) {
    VortexLauncher.launchLocal("Vortex_Example_AddOne", AddOneStart.class, 2, 1024, 4);
  }
}
```

* Through `VortexLauncher` user launches a Vortex job, passing following arguments
  * Name of the job 
  * Implementation of VortexStart(`AddOneStart`)
  * Amount of resources to use

```java
/**
 * AddOne User Code Example.
 */
final class AddOneStart implements VortexStart {
  @Inject
  public AddOneStart() {
  }

  @Override
  public void start(final VortexThreadPool vortexThreadPool) {
    final Vector<Integer> inputVector = new Vector<>();
    for (int i = 0; i < 1000; i++) {
      inputVector.add(i);
    }

    final List<VortexFuture<Integer>> futures = new ArrayList<>();
    final AddOneFunction addOneFunction = new AddOneFunction();
    for (final int i : inputVector) {
      futures.add(vortexThreadPool.submit(addOneFunction, i));
    }

    final Vector<Integer> outputVector = new Vector<>();
    for (final VortexFuture<Integer> future : futures) {
      try {
        outputVector.add(future.get());
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    }

    System.out.println("RESULT:");
    System.out.println(outputVector);
  }
}
```

* Through `VortexThreadPool#submit`, user submits a custom function(`AddOneFunction`) and its input to be executed on Vortex runtime
* Using returned `VortexFuture`, user retrieves execution results

```java
/**
 * Outputs input + 1.
 */
final class AddOneFunction implements VortexFunction<Integer, Integer> {
  @Override
  public Integer call(final Integer input) throws Exception {
    return input + 1;
  }
}
```
* User implementation of VortexFunction, takes an integer as the input and outputs input+1



 