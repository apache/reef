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

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.ConfigurationBuilder;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.ClassHierarchyException;
import org.apache.reef.tang.implementation.ConfigurationBuilderImpl;
import org.apache.reef.tang.implementation.ConfigurationImpl;
import org.apache.reef.tang.types.ClassNode;
import org.apache.reef.tang.types.ConstructorArg;
import org.apache.reef.tang.types.NamedParameterNode;
import org.apache.reef.tang.types.Node;
import org.apache.reef.tang.util.ReflectionUtilities;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.*;
import java.util.Map.Entry;

/**
 * @deprecated in Tang 0.2 Use AvroConfigurationSerializer instead.
 */
@Deprecated
public class ConfigurationFile {

  /**
   * Write Configuration to the given File.
   *
   * @throws IOException
   * @deprecated in Tang 0.2 Use AvroConfigurationSerializer instead.
   */
  @Deprecated
  public static void writeConfigurationFile(
      final Configuration conf, final File confFile) throws IOException {
    try (final PrintStream printStream = new PrintStream(new FileOutputStream(confFile))) {
      printStream.print(toConfigurationString(conf));
    }
  }

  /**
   * @deprecated in Tang 0.2 Use AvroConfigurationSerializer instead.
   */
  @Deprecated
  public static void addConfiguration(final ConfigurationBuilder conf,
                                      final File tmpConfFile) throws IOException, BindException {
    final PropertiesConfiguration confFile;
    try {
      confFile = new PropertiesConfiguration(tmpConfFile);
    } catch (final ConfigurationException e) {
      throw new BindException("Problem parsing config file: " + tmpConfFile, e);
    }
    processConfigFile(conf, confFile);
  }

  /**
   * @param conf     This configuration builder will be modified to incorporate the
   *                 contents of the configuration file.
   * @param contents A string containing the contents of the configuration file.
   * @throws BindException
   * @deprecated in Tang 0.2 Use AvroConfigurationSerializer instead.
   */
  @Deprecated
  public static void addConfiguration(final ConfigurationBuilder conf,
                                      final String contents) throws BindException {
    File tmpConfFile = null;
    try {
      tmpConfFile = File.createTempFile("tang", "tmp");
      try (final FileOutputStream outStream = new FileOutputStream(tmpConfFile)) {
        outStream.write(contents.getBytes());
      }
      addConfiguration(conf, tmpConfFile);
    } catch (final IOException ex) {
      throw new BindException("Error writing config file: " + tmpConfFile, ex);
    } finally {
      if (tmpConfFile != null) {
        tmpConfFile.delete();
      }
    }
  }

  private static void processConfigFile(ConfigurationBuilder conf,
                                        PropertiesConfiguration confFile) throws IOException, BindException {
    ConfigurationBuilderImpl ci = (ConfigurationBuilderImpl) conf;
    Iterator<String> it = confFile.getKeys();
    Map<String, String> importedNames = new HashMap<>();

    while (it.hasNext()) {
      String key = it.next();
      String longName = importedNames.get(key);
      String[] values = confFile.getStringArray(key);
      if (longName != null) {
        // System.err.println("Mapped " + key + " to " + longName);
        key = longName;
      }
      for (String value : values) {
        try {
          if (key.equals(ConfigurationBuilderImpl.IMPORT)) {
            ci.getClassHierarchy().getNode(value);
            final String[] tok = value.split(ReflectionUtilities.regexp);
            final String lastTok = tok[tok.length - 1];
            try {
              // ci.namespace.getNode(lastTok);
              ci.getClassHierarchy().getNode(lastTok);
              throw new IllegalArgumentException("Conflict on short name: " + lastTok);
            } catch (BindException e) {
              String oldValue = importedNames.put(lastTok, value);
              if (oldValue != null) {
                throw new IllegalArgumentException("Name conflict: "
                    + lastTok + " maps to " + oldValue + " and " + value);
              }
            }
          } else if (value.startsWith(ConfigurationBuilderImpl.INIT)) {
            String parseValue = value.substring(
                ConfigurationBuilderImpl.INIT.length(), value.length());
            parseValue = parseValue.replaceAll("^[\\s\\(]+", "");
            parseValue = parseValue.replaceAll("[\\s\\)]+$", "");
            String[] classes = parseValue.split("[\\s\\-]+");
            ci.registerLegacyConstructor(key, classes);
          } else {
            ci.bind(key, value);
          }
        } catch (BindException e) {
          throw new BindException("Failed to process configuration tuple: [" + key + "=" + value + "]", e);
        } catch (ClassHierarchyException e) {
          throw new ClassHierarchyException("Failed to process configuration tuple: [" + key + "=" + value + "]", e);
        }
      }
    }
  }

  /**
   * Replace any \'s in the input string with \\. and any "'s with \".
   *
   * @param in
   * @return
   */
  private static String escape(String in) {
    // After regexp escaping \\\\ = 1 slash, \\\\\\\\ = 2 slashes.

    // Also, the second args of replaceAll are neither strings nor regexps, and
    // are instead a special DSL used by Matcher. Therefore, we need to double
    // escape slashes (4 slashes) and quotes (3 slashes + ") in those strings.
    // Since we need to write \\ and \", we end up with 8 and 7 slashes,
    // respectively.
    return in.replaceAll("\\\\", "\\\\\\\\").replaceAll("\"", "\\\\\\\"");
  }

  /**
   * Obtain the effective configuration of this ConfigurationBuilderImpl
   * instance. This consists of string-string pairs that could be written
   * directly to a Properties file, for example. Currently, this method does not
   * return information about default parameter values that were specified by
   * parameter annotations, or about the auto-discovered stuff in TypeHierarchy.
   * All of that should be automatically imported as these keys are parsed on
   * the other end.
   *
   * @return A string containing enough information to rebuild this
   * configuration object (assuming the same classes / jars are
   * available when the string is parsed by Tang).
   * @deprecated in Tang 0.2 Use AvroConfigurationSerializer instead.
   */
  @Deprecated
  public static String toConfigurationString(final Configuration c) {
    StringBuilder sb = new StringBuilder();
    for (String s : toConfigurationStringList(c)) {
      sb.append(s);
      sb.append('\n');
    }
    return sb.toString();
  }

  /**
   * @deprecated in Tang 0.2 Use AvroConfigurationSerializer instead.
   */
  @Deprecated
  static List<String> toConfigurationStringList(final Configuration c) {
    ConfigurationImpl conf = (ConfigurationImpl) c;
    List<String> l = new ArrayList<>();
    for (ClassNode<?> opt : conf.getBoundImplementations()) {
      l.add(opt.getFullName()
          + '='
          + escape(conf.getBoundImplementation(opt).getFullName()));
    }
    for (ClassNode<?> opt : conf.getBoundConstructors()) {
      l.add(opt.getFullName()
          + '='
          + escape(conf.getBoundConstructor(opt).getFullName()));
    }
    for (NamedParameterNode<?> opt : conf.getNamedParameters()) {
      l.add(opt.getFullName()
          + '='
          + escape(conf.getNamedParameter(opt)));
    }
    for (ClassNode<?> cn : conf.getLegacyConstructors()) {
      StringBuilder sb = new StringBuilder();
      join(sb, "-", conf.getLegacyConstructor(cn).getArgs());
      l.add(cn.getFullName()
          + escape('='
              + ConfigurationBuilderImpl.INIT
              + '('
              + sb.toString()
              + ')'
      ));
      //s.append(cn.getFullName()).append('=').append(ConfigurationBuilderImpl.INIT).append('(');
//      .append(")\n");
    }
    for (Entry<NamedParameterNode<Set<?>>, Object> e : conf.getBoundSets()) {
      final String val;
      if (e.getValue() instanceof String) {
        val = (String) e.getValue();
      } else if (e.getValue() instanceof Node) {
        val = ((Node) e.getValue()).getFullName();
      } else {
        throw new IllegalStateException();
      }
      l.add(e.getKey().getFullName() + '=' + escape(val));
//      s.append(e.getKey().getFullName()).append('=').append(val).append("\n");
    }
    return l;//s.toString();
  }

  private static StringBuilder join(final StringBuilder sb, final String sep, final ConstructorArg[] types) {
    if (types.length > 0) {
      sb.append(types[0].getType());
      for (int i = 1; i < types.length; i++) {
        sb.append(sep).append(types[i].getType());
      }
    }
    return sb;
  }
}
