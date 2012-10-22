Tang
====

Tang is a simple dependency injection framework that emphasizes configuration
and explicit documentation over executing application-specific code when binding
implementations and invoking object constructors.

Passing configuration parameters to objects
-------------------------------------------

Suppose you are implementing a new class, and would like to 
automatically pass configuration parameters to it at runtime:

''''java
package com.example;
class Timer {
  int seconds;
  public Timer(int seconds) { this.seconds = seconds; }
  public wait throws Exception ( java.lang.Thread.sleep(seconds); }
}
''''
