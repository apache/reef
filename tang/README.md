Tang
====

Tang is a simple dependency injection framework that emphasizes configuration
and explicit documentation over executing application-specific code when binding
implementations and invoking object constructors.

Passing configuration parameters to objects
-------------------------------------------

Suppose you are implementing a new class, and would like to 
automatically pass configuration parameters to it at runtime:

```java
package com.example;

public class Timer {
  private final int seconds;

  public Timer(int seconds) {
    this.seconds = seconds;
  }

  public void sleep() throws Exception {
    java.lang.Thread.sleep(seconds * 1000);
  }
}
```
Tang encourages applications to use Plain Old Java (POJO) objects, and emphasizes the
use of immutable state for configuration parameters.  This reduces boiler plate (there is
no need for extra setter methods), and does not interfere with encapsulation (the 
parameters are private).  Furthermore, it is trivial for well-written classes to ensure
that all objects are completely and properly instantiated; they simply need to check 
constructor parameters as any other POJO would.

In order for Tang to instantiate an object, we need to annotate the constructor with an
@Inject annotation.  While we're out it, we'll define a configuration parameter, allowing
us to specify seconds on the command line, and in a config file.

```java
package com.example;

import javax.inject.Inject;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;

public class Timer {
  @NamedParameter(type=int.class, default_value="10",
      doc="Number of seconds to sleep", short_name="sec")
  class Seconds implements Name {}
  private final int seconds;

  @Inject
  public Timer(@Parameter(Seconds.class) int seconds) {
    this.seconds = seconds;
  }

  public void sleep() throws Exception {
    java.lang.Thread.sleep(seconds * 1000);
  }
}
```
A few things happened here.  First, we create the new configuration parameter
by declaring a dummy class that implements Tang's "Name" interface.  This class
is annotated with "@NamedParameter", which specifies:

 * type: The type of the configuration parameter, which must match the type taken by the constructor.  This is needed because one @NamedParameter may be passed into multiple constructors.
 * default_value (optional): The default value of the constructor parameter, encoded as a string.  Tang will parse this value (and ones in config files, and on the command line), and pass it into the constructor.
 * short_name (optional): The name of the command line parameter.
 * doc (optional): Human readable documentation, describing the purpose of the parameter.

Next, the @Inject annotation flags the constructor so that Tang will consider it
when attempting to instantiate this class.  Finally, the @Parameter annotation
takes the class associated with the configuration parameter.  Using a dummy class
allows IDEs to autocomplete configuration parameter names, and lets the compiler
confirm them as well.

Of course, in isolation, this example is incomplete; we need a main() method to 
invoke tang, and instantiate our timer:

```java
...
import com.microsoft.tang.Tang;
import com.microsoft.tang.TypeHierarchy;
...
  public static void main(String[] args) throws Exception {
    TypeHierarchy typeHierarchy = new TypeHierarchy();
    typeHierarchy.register(Timer.class);
    Tang tang = new Tang(typeHierarchy);
    Timer timer = tang.getInstance(Timer.class);
    System.out.println("Tick...");
    timer.sleep();
    System.out.println("Tock.");
  }
```

In order to use Tang, we first build a database of classes that it should consider for use at runtime.
We do this by instantiating a new TypeHierarchy object.  The only class we're interested in is our new Timer,
so we pass Timer.class into typeHierarchy.register().  Next, we pass typeHierarchy into Tang's constructor,
and ask Tang to instantiate a new Timer object.  Tang automatically passes 10, the default_value field of
Seconds into Timer's constructor, so the call to sleep() takes 10 seconds to complete.

Looking under the hood
----------------------

In the example above, TypeHierarchy walks the class definition for Timer, looking
for superclasses, interfaces, and classes referenced by its constructors.  In this case, there is nothing too
interesting to find.  To take a look, add a call to '''typeHierarchy.writeJson(System.err)''' after the call
to register.  You should see this on stderr:

```json
{
  "namespace" : {
    "name" : "",
    "children" : [ {
      "name" : "com",
      "children" : [ {
        "name" : "example",
        "children" : [ {
          "name" : "Timer",
          "children" : [ {
            "name" : "Seconds",
            "children" : [ ],
            "shortName" : "sec"
          } ]
        } ]
      }, {
        "name" : "microsoft",
        "children" : [ {
          "name" : "tang",
          "children" : [ {
            "name" : "annotations",
            "children" : [ {
              "name" : "Name",
              "children" : [ ]
            } ]
          } ]
        } ]
      } ]
    }, {
      "name" : "java",
      "children" : [ {
        "name" : "lang",
        "children" : [ {
          "name" : "Object",
          "children" : [ ]
        } ]
      } ]
    } ]
  },
  "namedParameterNodes" : [ {
    "name" : "Seconds",
    "children" : [ ],
    "shortName" : "sec"
  } ]
```
This is quite verbose, but is simply saying that Tang found (in order): com.example.Timer,
com.example.Timer.Seconds (note that Tang's state mirrors the Java package hierarchy, so
Timer is an ancestor of Seconds).  It also pulled in Seconds' superclass,
com.microsoft.tang.annotations.Name, and java.lang.Object.  A second JSON element,
"namedParameterNodes" documents the named parameters that have been discovered so far.  

Note that, as of the writing of this document, the JSON format is incomplete, and is
expected to change over time.