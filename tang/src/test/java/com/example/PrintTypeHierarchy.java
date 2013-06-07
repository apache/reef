/*
 * Copyright 2013 Microsoft.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.example;

import javax.inject.Inject;

import com.microsoft.tang.Tang;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.ConfigurationBuilder;
import com.microsoft.tang.Injector;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;

import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.CommandLine;

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
