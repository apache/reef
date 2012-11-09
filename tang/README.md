Tang is a simple dependency injection framework that emphasizes configuration
and explicit documentation over executing application-specific code when binding
implementations and invoking object constructors.

Outline
-------

   * [Introduction](#introduction)
   * [Defining configuration parameters](#configuration)
   * [Instantiating objects with Tang](#injection)
   * [Specifying default configurations](#static-configuration)
   * [Creating sets of similar injectors](#child-injectors)
   * [Dynamically setting parameters and choosing implementations](#bind)
   * [Distributed dependency injection](#distributed-dependency-injection)
   * [Using the injection plan API to choose between multiple implementations](#injection-plans)
   * [Language interoperability](#language-interoperability)

Introduction
============

Tang is a new type of dependency injection framework.  It encourages application developers to specify default implementations and constructor parameters in terms of configurations, avoiding the need for a number of subtle (and often confusing) dependency injection software patterns. Tang's configurations are "just data," and can be read and written in human readable formats.  Furthermore, they include documentation facilities, and automatic command line and configuration file parsing.  From an end-user perspective, this takes a lot of the guesswork out of configuration file formats.

Although Tang surfaces a text-based interface for end-users of the applications built atop it, all configuration options and their types are specified in terms of Java classes and annotations.  This allows the Java compiler to statically check a range of properties important to Tang, eliminating broad classes of runtime errors.   Furthermore, upon loading a class that will be involved in dependency injection, Tang performs a number of additional consistency checks, allowing it to detect inconsistent usage of configuration parameters, naming conflicts and so on.  These checks can be run independently of the application's runtime environment, and can find problems both in the Java-level implementation of the system, and with user-provided configuration files.  The tools (v2) [1] that perform these checks are designed to run as a post-processing step of projects built atop Tang.  Like the Java compiler checks, this prevents such errors from making it to production environments.  It also prevents such errors from being exposed to application logic or end-users, greatly simplifying applications built atop Tang.

In addition to improving end-user's interactions with applications, our configuration-centric approach to dependency injection enables a number of advanced features not supported by existing dependency injection frameworks:

   * Distributed injection: Tang's InjectionPlan API allows machines to compute injection plans locally, based on code that will be shipped and run elsewhere.  These injection plans can be checked for feasibility and then shipped to the machine that will run the code in question.  In keeping with Tang's philosophy of catching errors early, this limits the responsibility of handling most dependency injection problems to a single software component running on a single machine.
   * Multi-language injection (v2):  Tang's TypeHierarchy API encodes the relationship between interfaces, implementation classes, and constructors in a language-independent fashion.  Computation of InjectionPlan objects (and the related consistency checks) occurs atop this language-independent abstraction, allowing Injection Plans that span language boundaries.
   * Ambiguous injection (v2): Tang injection plans are encoded in a format that is designed to cope with ambiguous injection plans.  In situations where more than one implementation is available, Tang allows the code that invoked it to examine the possibilities, and choose the most appropriate course of action.

Taken together, these properties greatly simplify dependency injection in distributed environments.  Tang eliminates large classes of runtime configuration problems (which can be extremely difficult to debug in distributed environments), and provides facilities to make it easy for higher-level code to use information about the current runtime environment to choose the appropriate course of action.  

We expect Tang to be used in environments that are dominated by "plugin"-style APIs with many alternative implementations.  Tang cleanly separates concerns over configuration management, dependency injection, and object implementations.  This hides most of the complexity of dependency injection from plugin implementers.  It also prevents plugin implementations from inadvertently breaking the high-level semantics that allow Tang to perform extensive static checking and to provide clean semantics in distributed, heterogeneous environments.

Tutorial
========

Configuration
-------------

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
Tang encourages applications to use Plain Old Java (POJO) objects, and emphasizes the use of immutable state for configuration parameters.  This reduces boiler plate (there is no need for extra setter methods), and does not interfere with encapsulation (the fields, and even the constructor can be private).  Furthermore, it is trivial for well-written classes to ensure that all objects are completely and properly instantiated; they simply need to check constructor parameters as any other POJO would.

In order for Tang to instantiate an object, we need to annotate the constructor with an @Inject annotation.  While we're at it, we'll define a configuration parameter, allowing us to specify seconds on the command line, and in a config file.

```java
package com.example;

import javax.inject.Inject;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;

public class Timer {
  @NamedParameter(default_value="10",
      doc="Number of seconds to sleep", short_name="sec")
  class Seconds implements Name<Integer> {}
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
A few things happened here.  First, we create the new configuration parameter by declaring a dummy class that implements Tang's "Name" interface.  Name is a generic type, with a single mandatory parameter that specifies the type of object to be passed in.  So, the Seconds class declares a parameter called "Seconds" that expects Integer values.

All instances of Name must be annotated with @NamedParamter, which takes a number of options:
 * default_value (optional): The default value of the constructor parameter, encoded as a string.  Tang will parse this value (and ones in config files and on the command line), and pass it into the constructor.
 * short_name (optional): The name of the command line option associated with this parameter.  If ommitted, no command line option will be created.
 * doc (optional): Human readable documentation, describing the purpose of the parameter.

Next, the @Inject annotation flags the constructor so that Tang will consider it when attempting to instantiate this class.  Finally, the @Parameter annotation takes the class associated with the configuration parameter.  Using a dummy class allows IDEs to autocomplete configuration parameter names, and lets the compiler confirm them as well.

Injection
---------
Of course, in isolation, having the ability to specify configuration parameters is not particularly useful; at runtime, we need a way to invoke Tang, and to tell it to instantiate our objects.  This process is called _injection_.  Tang's injection process is designed to catch as many potential runtime errors as possible into checks that can be performed before application code begins to run.  This simplifies debugging, since many configurations can be caught without running (or examining) application-specific initialization code.  However, it does introduce a number of phases to Tang's initiliazation process and to injection:

```java
...
import com.microsoft.tang.Tang;
import com.microsoft.tang.ConfigurationBuilder;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Injector;
...
  public static void main(String[] args) throws Exception {
    Tang tang = Tang.Factory.getTang();
    ConfigurationBuilder cb = (ConfigurationBuilder)tang.newConfigurationBuilder();
    cb.register(Timer.class);
    Configuration conf = cb.build();
    Injector injector = tang.newInjector(conf);
    Timer timer = injector.getInstance(Timer.class);
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

Static configuration
--------------------

Child injectors
---------------

Bind
----

Distributed dependency injection
--------------------------------

Injection plans
---------------

Language interoperability
-------------------------

Passing configuration parameters to objects
-------------------------------------------





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
