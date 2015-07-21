# AllHandlers
This project contains an example of a REEF Program. It contains the following classes:

  * `AllHandlers`: This is the program that submits the driver to the local runtime.
  * `HelloDriverStartHandler`: The Driver requests 2 Evaluators.
  * `HelloAllocatedEvaluatorHandler` : The handler that submit Context and Tasks
  * `HelloActiveContextHandler` : The handler that submit Tasks to Context
  * `HelloHttpHandler` : The handler that handles http requests
  * `HelloRunningTaskHandler` : The handler that send messages to Running Task
  * `HelloTaskMessageHandler` : The handler that receives task messages
  *  Other handler samples
  * `HelloTask`: This Task prints a greeting to STDOUT of the Evaluator.

## Running it
Just run the main class, `AllHandlers`, followed by the runtime you want, e.g. `local` and the run time folder e.g. 'REEF_LOCAL_RUNTIME'. If you don't specify the run time parameters, default will be used. 
e.g. AllHandlers.exe local REEF_LOCAL_RUNTIME
or AllHandler.exe