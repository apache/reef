Tang
====

Tang is a simple dependency injection framework that emphasizes configuration
and explicit documentation over executing application-specific code when binding
implementations and invoking object constructors.


   * [Defining configuration parameters](#configuration)
   * [Instantiating objects with Tang](#injection)
   * [Specifying default configurations](#static-configuration)
   * [Creating sets of similar injectors](#child-injectors)
   * [Dynamically setting parameters and choosing implementations](#bind)
   * [Distributed dependency injection](#distributed-dependency-injection)
   * [Using the injection plan API to choose between multiple implementations](#injection-plans)
   * [Language interoperability](#language-interoperability)

Configuration
-------------

Injection
---------

Static configuration
--------------------

Child injectors
---------------

Bind
----

Distributed dependency injection
--------------------------------

Injection plan
--------------

Language interoperability
-------------------------



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

More complicated examples
-------------------------
Suppose we had multiple implementations of the timer example (TODO)

When things go wrong
--------------------
In the timer example, we specified a default value for the Sleep parameter.  If we hadn't done this, then the call
to getInstance() would have thrown an exception:
````
Exception in thread "main"
java.lang.IllegalArgumentException: Attempt to inject infeasible plan: com.example.Timer(int @Parameter(Seconds) null)
  at com.microsoft.tang.Tang.injectFromPlan(Tang.java:315)
	at com.microsoft.tang.Tang.getInstance(Tang.java:311)
	at com.example.Timer.main(Timer.java:35)
````
Since Tang refuses to inject null values into object
constructors, the plan to invoke Timer(null) is considered infeasible.  Note that this error message enumerates all
possible injection plans.  If Timer had more constructors or implementations
those would be enumerated here as well.  Similarly, if more than one feasible plan existed, Tang would refuse to perform
the injection, and throw a similar exception.

In both cases, the solution is to set Tang parameters to create a single feasible plan.  tang.setDefaultImpl() allows you
to hardcode Tang to use a particular implementation of a class/interface (and automatically registers its parameters with Tang).
Similarly, tang.setNamedParameter() lets you set named parameters, and override default values.

Looking under the hood
----------------------

### InjectionPlan

InjectionPlan objects explain what Tang would do to instantiate a new object, but don't actually instantiate anything.
Add the following lines to the Timer example;

````java
    InjectionPlan ip = tang.getInjectionPlan("com.example.Timer");
    System.out.println(InjectionPlan.prettyPrint(ip));
    System.out.println("Number of plans:" + ip.getNumAlternatives());
````

Running the program now produces a bit of additional output:
````
com.example.Timer(int @Parameter(Seconds) 10)
Number of plans:1
````

### TypeHierachy

InjectionPlan explains what would happen if you asked Tang to take some action, but it doesn't provide much insight
into Tang's view of the object hierarchy, parameter defaults and so on.  TypeHierarchy object encode
the state that Tang gets from .class files, including class inheritance relationships, parameter annotations, and so on.

In the example above, TypeHierarchy walks the class definition for Timer, looking
for superclasses, interfaces, and classes referenced by its constructors.  In this case, there is nothing too
interesting to find.  To take a look, add a call to '''typeHierarchy.writeJson(System.err)''' after the call
to register.  You should see something like this on stderr:

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
            "argClass" : "int",
            "defaultInstance" : 10,
            "shortName" : "sec",
            "documentation" : "Number of seconds to sleep",
            "nameClass" : "com.example.Timer$Seconds",
            "type" : "NamedParameterNode"
          } ],
          "clazz" : "com.example.Timer",
          "isPrefixTarget" : false,
          "injectableConstructors" : [ {
            "args" : [ {
              "type" : "int",
              "name" : "com.example.Timer$Seconds"
            } ],
            "constructor" : "com.example.Timer"
          } ],
          "type" : "ClassNode"
        } ],
        "type" : "PackageNode"
      }, {
        "name" : "microsoft",
        "children" : [ {
          "name" : "tang",
          "children" : [ {
            "name" : "annotations",
            "children" : [ {
              "name" : "Name",
              "children" : [ ],
              "clazz" : "com.microsoft.tang.annotations.Name",
              "isPrefixTarget" : false,
              "injectableConstructors" : [ ],
              "type" : "ClassNode"
            } ],
            "type" : "PackageNode"
          } ],
          "type" : "PackageNode"
        } ],
        "type" : "PackageNode"
      } ],
      "type" : "PackageNode"
    }, {
      "name" : "java",
      "children" : [ {
        "name" : "lang",
        "children" : [ {
          "name" : "Object",
          "children" : [ ],
          "clazz" : "java.lang.Object",
          "isPrefixTarget" : false,
          "injectableConstructors" : [ ],
          "type" : "ClassNode"
        } ],
        "type" : "PackageNode"
      } ],
      "type" : "PackageNode"
    } ],
    "type" : "PackageNode"
  },
  "namedParameterNodes" : [ {
    "name" : "Seconds",
    "children" : [ ],
    "argClass" : "int",
    "defaultInstance" : 10,
    "shortName" : "sec",
    "documentation" : "Number of seconds to sleep",
    "nameClass" : "com.example.Timer$Seconds",
    "type" : "NamedParameterNode"
  } ]
}
```
This is quite verbose, but is simply saying that Tang found (in order): com.example.Timer,
com.example.Timer.Seconds (note that Tang's state mirrors the Java package hierarchy, so
Timer is an ancestor of Seconds).  It also pulled in Seconds' superclass,
com.microsoft.tang.annotations.Name, and java.lang.Object.  A second JSON element,
"namedParameterNodes" documents the named parameters that have been discovered so far.  

Note that, as of the writing of this document, the JSON format is incomplete, and is
expected to change over time.

### Tang (TODO)

Tang objects encode state that is derived dynamically.  This include information from
command line parameters configuration files, and from application-level calls to
setDefaultImpl() and setNamedParameter().  Methods to dump / read this information are coming soon.
