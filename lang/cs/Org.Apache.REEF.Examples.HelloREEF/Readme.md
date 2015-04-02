# HelloREEF
This project contains a simple example of a REEF Program. It contains the following classes:

  * `HelloREEF`: This is the program that submits the driver to the local runtime.
  * `HelloDriver`: The Driver requests a single Evaluator and submits the `HelloTask` to it.
  * `HelloTask`: This Task prints a greeting to STDOUT of the Evaluator.

## Running it
Just run the main class, `HelloREEF`, followed by the runtime you want, e.g. `local`.