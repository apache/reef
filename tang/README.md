Tang is a simple dependency injection framework that emphasizes configuration
and explicit documentation over executing application-specific code when binding
implementations and invoking object constructors.

Outline
-------

   * [Introduction](#introduction)
   * [Defining configuration parameters](#configuration-parameters)
   * [Instantiating objects with Tang](#injection)
   * [Processing configuration options](#processing-configurations)
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

Configuration Parameters
------------------------

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
Of course, in isolation, having the ability to specify configuration parameters is not particularly useful; at runtime, we need a way to invoke Tang, and to tell it to instantiate our objects.  This process is called _injection_.  Tang's injection process is designed to catch as many potential runtime errors as possible into checks that can be performed before application code begins to run.  This simplifies debugging, since many configurations can be caught without running (or examining) application-specific initialization code.  However, it does introduce a number of new phases to Tang's initiliazation process and to injection:

```java
...
import com.microsoft.tang.Tang;
import com.microsoft.tang.ConfigurationBuilder;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Injector;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;

...
  public static void main(String[] args) throws BindException, InjectionException {
    Tang tang = Tang.Factory.getTang();
    ConfigurationBuilder cb = (ConfigurationBuilder)tang.newConfigurationBuilder();
    cb.register(Timer.class);
    Configuration conf = cb.build();
    Injector injector = tang.newInjector(conf);
    Timer timer = injector.getInstance(Timer.class);
    
    try {
      System.out.println("Tick...");
      timer.sleep();
      System.out.println("Tock.");
    } catch(InterruptedException e) {
      e.printStackTrace();
    }
  }
```
The first step in using Tang is to get a handle to a Tang object by calling "Tang.Factory.getTang()".  Having obtained a handle, we run through each of the phases of a Tang injection:
   * We use _ConfigurationBuilder_ objects to tell Tang about the class hierarchy that it will be using to inject objects and (in later examples) to register the contents of configuration files, override default configuration values, and to set default implementations of classes.
   * For this example, we simply call the cb.register(Class<?>) method in ConfigurationBuilder.  We pass in Timer.class, which tells Tang to process Timer, as well as any superclasses and internal classes (such as our parameter class, Seconds).
   * We call build() on the ConfigurationBuilder, creating an immutable Configuration object.  At this point, Tang ensures that all of the classes it has encountered so far are consistent with each other, and that they are suitable for injection.  When Tang encounters conflicting classes or configuration files, it throws a BindException to indicate that the problem is due to configuration issues. Note that ConfigurationBuilder and Configuration do not determine whether or not a particular injection will succeed; that is the business of the _Injector_.
   * To obtain an instance of Injector, we pass our Configuration object into tang.newInjector().
   * Finally, we call injector.getInstance(Timer.class).  Internally, this method considers all possible injection plans for Timer.class.  If there is exactly one such plan, it performs the injection.  Otherwise, it throws an InjectionException.

Processing configurations
-------------------------

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

In both cases, the solution is to set additional configuration parameters to create a single feasible plan.  This can be done using any of the methods described above.

Looking under the hood
----------------------

### InjectionPlan

InjectionPlan objects explain what Tang would do to instantiate a new object, but don't actually instantiate anything.
Add the following lines to the Timer example;

````java
import com.microsoft.tang.implementation.InjectionPlan;
import com.microsoft.tang.implementation.InjectorImpl;
...
    InjectorImpl injector = (InjectorImpl)tang.newInjector(conf);
    InjectionPlan<Timer> ip = injector.getInjectionPlan(Timer.class);
    System.out.println(ip.toPrettyString());
    System.out.println("Number of plans:" + ip.getNumAlternatives());
````

Running the program now produces a bit of additional output:
````
new Timer(Integer Seconds = 10)
Number of plans:1
````

### TypeHierachy

InjectionPlan explains what would happen if you asked Tang to take some action, but it doesn't provide much insight
into Tang's view of the object hierarchy, parameter defaults and so on.  TypeHierarchy object encode
the state that Tang gets from .class files, including class inheritance relationships, parameter annotations, and so on.

Internally, in the example above, TypeHierarchy walks the class definition for Timer, looking
for superclasses, interfaces, and classes referenced by its constructors.