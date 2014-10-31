/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.tang.formats;

import org.apache.commons.cli.*;
import org.apache.reef.tang.ConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.NameResolutionException;
import org.apache.reef.tang.types.NamedParameterNode;
import org.apache.reef.tang.types.Node;
import org.apache.reef.tang.util.MonotonicTreeMap;
import org.apache.reef.tang.util.ReflectionUtilities;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public final class CommandLine {

  final Map<Option, CommandLineCallback> applicationOptions = new HashMap<>();
  private final ConfigurationBuilder conf;
  private final Map<String, String> shortNames = new MonotonicTreeMap<>();

  public CommandLine() {
    this.conf = Tang.Factory.getTang().newConfigurationBuilder();
  }

  public CommandLine(final ConfigurationBuilder conf) {
    this.conf = conf;
  }

  public ConfigurationBuilder getBuilder() {
    return this.conf;
  }

  public CommandLine registerShortNameOfClass(final String s) throws BindException {

    final Node n;
    try {
      n = conf.getClassHierarchy().getNode(s);
    } catch (final NameResolutionException e) {
      throw new BindException("Problem loading class " + s, e);
    }

    if (n instanceof NamedParameterNode) {
      final NamedParameterNode<?> np = (NamedParameterNode<?>) n;
      final String shortName = np.getShortName();
      final String longName = np.getFullName();
      if (shortName == null) {
        throw new BindException(
            "Can't register non-existent short name of named parameter: " + longName);
      }
      shortNames.put(shortName, longName);
    } else {
      throw new BindException("Can't register short name for non-NamedParameterNode: " + n);
    }

    return this;
  }

  public CommandLine registerShortNameOfClass(
      final Class<? extends Name<?>> c) throws BindException {
    return registerShortNameOfClass(ReflectionUtilities.getFullName(c));
  }

  @SuppressWarnings("static-access")
  private Options getCommandLineOptions() {

    final Options opts = new Options();
    for (final String shortName : shortNames.keySet()) {
      final String longName = shortNames.get(shortName);
      try {
        opts.addOption(OptionBuilder
            .withArgName(conf.classPrettyDefaultString(longName)).hasArg()
            .withDescription(conf.classPrettyDescriptionString(longName))
            .create(shortName));
      } catch (final BindException e) {
        throw new IllegalStateException(
            "Could not process " + shortName + " which is the short name of " + longName, e);
      }
    }

    for (final Option o : applicationOptions.keySet()) {
      opts.addOption(o);
    }

    return opts;
  }

  public CommandLine addCommandLineOption(final Option option, final CommandLineCallback cb) {
    // TODO: Check for conflicting options.
    applicationOptions.put(option, cb);
    return this;
  }

  /**
   * @param args
   * @return Selfie if the command line parsing succeeded, null (or exception) otherwise.
   * @throws IOException
   * @throws NumberFormatException
   * @throws ParseException
   */
  @SafeVarargs
  final public <T> CommandLine processCommandLine(
      final String[] args, Class<? extends Name<?>>... argClasses)
      throws IOException, BindException {

    for (final Class<? extends Name<?>> c : argClasses) {
      registerShortNameOfClass(c);
    }

    final Options o = getCommandLineOptions();
    o.addOption(new Option("?", "help"));
    final Parser g = new GnuParser();

    final org.apache.commons.cli.CommandLine cl;
    try {
      cl = g.parse(o, args);
    } catch (final ParseException e) {
      throw new IOException("Could not parse config file", e);
    }

    if (cl.hasOption("?")) {
      new HelpFormatter().printHelp("reef", o);
      return null;
    }

    for (final Option option : cl.getOptions()) {

      final String shortName = option.getOpt();
      final String value = option.getValue();

      if (applicationOptions.containsKey(option)) {
        applicationOptions.get(option).process(option);
      } else {
        try {
          conf.bind(shortNames.get(shortName), value);
        } catch (final BindException e) {
          throw new BindException("Could not bind shortName " + shortName + " to value " + value, e);
        }
      }
    }

    return this;
  }

  public interface CommandLineCallback {
    public void process(final Option option);
  }
}
