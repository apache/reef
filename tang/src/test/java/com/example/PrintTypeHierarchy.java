package com.example;

import javax.inject.Inject;

import com.microsoft.tang.Tang;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.ConfigurationBuilder;
import com.microsoft.tang.Injector;
import com.microsoft.tang.JavaConfigurationBuilder;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;

import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.CommandLine;
import com.microsoft.tang.implementation.InjectionPlan;
import com.microsoft.tang.implementation.java.InjectorImpl;

import com.microsoft.tang.util.walk.Walk;
import com.microsoft.tang.util.walk.GraphVisitor;
import com.microsoft.tang.util.walk.GraphVisitorGraphviz;
import java.io.IOException;

/**
 * Prints sample configuration in Graphviz DOT format to stdout.
 * @author sears, sergiym
 */
public final class PrintTypeHierarchy {

  /** Parameter to test the injection. */
  @NamedParameter(default_value = "999", doc = "Test parameter", short_name = "id")
  class Id implements Name<Integer> { }

  /** Parameter to test the injection. */
  private final transient int mId;

  /**
   * Constructor to test the parameter injection.
   * @param aId test parameter
   */
  @Inject
  public PrintTypeHierarchy(@Parameter(PrintTypeHierarchy.Id.class) final int aId) {
    this.mId = aId;
  }

  /**
   * @return string representation of the object.
   */
  @Override
  public String toString() {
    return this.getClass().getName() + " :: " + this.mId;
  }

  /**
   * @param aArgs command line arguments.
   * @throws BindException configuration error.
   * @throws InjectionException configuration error.
   * @throws IOException cannot process command line parameters.
   */
  public static void main(final String[] aArgs)
    throws BindException, InjectionException, IOException
  {

    final Tang tang = Tang.Factory.getTang();
    final ConfigurationBuilder confBuilder = tang.newConfigurationBuilder();

    new CommandLine(confBuilder).processCommandLine(aArgs);
    final Configuration config = confBuilder.build();

    final Injector injector = tang.newInjector(config);
    final PrintTypeHierarchy myself = injector.getInstance(PrintTypeHierarchy.class);

    final GraphVisitor visitor = new GraphVisitorGraphviz(config);
    Walk.preorder(visitor, config);

    System.out.println(visitor);
    System.out.println(myself);
  }
}
