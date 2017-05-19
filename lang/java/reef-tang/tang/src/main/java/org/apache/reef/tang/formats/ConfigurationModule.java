/*
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

import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.ClassHierarchyException;
import org.apache.reef.tang.exceptions.NameResolutionException;
import org.apache.reef.tang.implementation.ConfigurationBuilderImpl;
import org.apache.reef.tang.implementation.ConfigurationImpl;
import org.apache.reef.tang.types.ClassNode;
import org.apache.reef.tang.types.ConstructorArg;
import org.apache.reef.tang.types.NamedParameterNode;
import org.apache.reef.tang.types.Node;
import org.apache.reef.tang.util.*;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Allows applications to bundle sets of configuration options together into
 * discrete packages.  Unlike more conventional approaches,
 * ConfigurationModules store such information in static data structures that
 * can be statically discovered and sanity-checked.
 *
 * See org.apache.reef.tang.formats.TestConfigurationModule for more information and examples.
 */
public class ConfigurationModule {

  private static final Logger LOG = Logger.getLogger(ConfigurationModule.class.getName());

  private final ConfigurationModuleBuilder builder;
  // Set of required unset parameters. Must be empty before build.
  private final Set<Field> reqSet = new MonotonicHashSet<>();
  private final Map<Impl<?>, Class<?>> setImpls = new MonotonicHashMap<>();
  private final MonotonicMultiHashMap<Impl<?>, Class<?>> setImplSets = new MonotonicMultiHashMap<>();
  private final MonotonicMultiHashMap<Impl<?>, String> setLateImplSets = new MonotonicMultiHashMap<>();
  private final MonotonicMultiHashMap<Param<?>, String> setParamSets = new MonotonicMultiHashMap<>();
  private final Map<Impl<?>, String> setLateImpls = new MonotonicHashMap<>();
  private final Map<Param<?>, String> setParams = new MonotonicHashMap<>();
  private final Map<Impl<List>, List<?>> setImplLists = new MonotonicHashMap<>();
  private final Map<Param<List>, List<?>> setParamLists = new MonotonicHashMap<>();

  protected ConfigurationModule(final ConfigurationModuleBuilder builder) {
    this.builder = builder.deepCopy();
  }

  private ConfigurationModule deepCopy() {
    final ConfigurationModule cm = new ConfigurationModule(builder.deepCopy());
    cm.setImpls.putAll(setImpls);
    cm.setImplSets.addAll(setImplSets);
    cm.setLateImplSets.addAll(setLateImplSets);
    cm.setParamSets.addAll(setParamSets);
    cm.setLateImpls.putAll(setLateImpls);
    cm.setParams.putAll(setParams);
    cm.reqSet.addAll(reqSet);
    cm.setImplLists.putAll(setImplLists);
    cm.setParamLists.putAll(setParamLists);
    return cm;
  }

  private <T> void processSet(final Object impl) {
    final Field f = builder.map.get(impl);
    if (f == null) { /* throw */
      throw new ClassHierarchyException("Unknown Impl/Param when setting " +
          ReflectionUtilities.getSimpleName(impl.getClass()) + ".  Did you pass in a field from some other module?");
    }
    if (!reqSet.contains(f)) {
      reqSet.add(f);
    }
  }

  public final <T> ConfigurationModule set(final Impl<T> opt, final Class<? extends T> impl) {
    final ConfigurationModule c = deepCopy();
    c.processSet(opt);
    if (c.builder.setOpts.contains(opt)) {
      c.setImplSets.put(opt, impl);
    } else {
      c.setImpls.put(opt, impl);
    }
    return c;
  }

  public final <T> ConfigurationModule set(final Impl<T> opt, final String impl) {
    final ConfigurationModule c = deepCopy();
    c.processSet(opt);
    if (c.builder.setOpts.contains(opt)) {
      c.setLateImplSets.put(opt, impl);
    } else {
      c.setLateImpls.put(opt, impl);
    }
    return c;
  }

  /**
   * Binds a list to a specific optional/required Impl using ConfigurationModule.
   *
   * @param opt      Target optional/required Impl
   * @param implList List object to be injected
   * @param <T> a type
   * @return the configuration module
   */
  public final <T> ConfigurationModule set(final Impl<List> opt, final List implList) {
    final ConfigurationModule c = deepCopy();
    c.processSet(opt);
    c.setImplLists.put(opt, implList);
    return c;
  }

  public final <T> ConfigurationModule set(final Param<T> opt, final Class<? extends T> val) {
    return set(opt, ReflectionUtilities.getFullName(val));
  }

  public final ConfigurationModule set(final Param<Boolean> opt, final boolean val) {
    return set(opt, "" + val);
  }

  public final ConfigurationModule set(final Param<? extends Number> opt, final Number val) {
    return set(opt, "" + val);
  }

  public final <T> ConfigurationModule set(final Param<T> opt, final String val) {
    final ConfigurationModule c = deepCopy();
    c.processSet(opt);
    if (c.builder.setOpts.contains(opt)) {
      c.setParamSets.put(opt, val);
    } else {
      c.setParams.put(opt, val);
    }
    return c;
  }

  /**
   * Binds a set of values to a Param using ConfigurationModule.
   *
   * @param opt    Target Param
   * @param values Values to bind to the Param
   * @param <T> type
   * @return the Configuration module
   */
  public final <T> ConfigurationModule setMultiple(final Param<T> opt, final Iterable<String> values) {
    ConfigurationModule c = deepCopy();
    for (final String val : values) {
      c = c.set(opt, val);
    }
    return c;
  }

  /**
   * Binds a set of values to a Param using ConfigurationModule.
   *
   * @param opt    Target Param
   * @param values Values to bind to the Param
   * @param <T> type
   * @return the Configuration module
   */
  public final <T> ConfigurationModule setMultiple(final Param<T> opt, final String... values) {
    ConfigurationModule c = deepCopy();
    for (final String val : values) {
      c = c.set(opt, val);
    }
    return c;
  }

  /**
   * Binds a list to a specific optional/required Param using ConfigurationModule.
   *
   * @param opt      target optional/required Param
   * @param implList List object to be injected
   * @param <T>      type
   * @return the Configuration module
   */
  public final <T> ConfigurationModule set(final Param<List> opt, final List implList) {
    final ConfigurationModule c = deepCopy();
    c.processSet(opt);
    c.setParamLists.put(opt, implList);
    return c;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public Configuration build() throws BindException {
    final ConfigurationModule c = deepCopy();

    //TODO[REEF-968] check that required parameters have not been set to null

    if (!c.reqSet.containsAll(c.builder.reqDecl)) {
      final Set<Field> missingSet = new MonotonicHashSet<>();
      for (final Field f : c.builder.reqDecl) {
        if (!c.reqSet.contains(f)) {
          missingSet.add(f);
        }
      }
      throw new BindException(
          "Attempt to build configuration before setting required option(s): "
              + builder.toString(missingSet));
    }

    for (final Class<?> clazz : c.builder.freeImpls.keySet()) {
      final Impl<?> i = c.builder.freeImpls.get(clazz);
      boolean foundOne = false;
      if (c.setImpls.containsKey(i)) {
        if (c.setImpls.get(i) != null) {
          c.builder.b.bind(clazz, c.setImpls.get(i));
          foundOne = true;
        }
      } else if (c.setLateImpls.containsKey(i)) {
        if (c.setLateImpls.get(i) != null) {
          c.builder.b.bind(ReflectionUtilities.getFullName(clazz), c.setLateImpls.get(i));
          foundOne = true;
        }
      } else if (c.setImplSets.containsKey(i) || c.setLateImplSets.containsKey(i)) {
        if (c.setImplSets.getValuesForKey(i) != null) {
          for (final Class<?> clz : c.setImplSets.getValuesForKey(i)) {
            c.builder.b.bindSetEntry((Class) clazz, (Class) clz);
          }
          foundOne = true;
        }
        if (c.setLateImplSets.getValuesForKey(i) != null) {
          for (final String s : c.setLateImplSets.getValuesForKey(i)) {
            c.builder.b.bindSetEntry((Class) clazz, s);
          }
          foundOne = true;
        }
      } else if (c.setImplLists.containsKey(i)) {
        if (c.setImplLists.get(i) != null) {
          c.builder.b.bindList((Class) clazz, c.setImplLists.get(i));
          foundOne = true;
        }
      }
      if(!foundOne && !(i instanceof  OptionalImpl)) {
        final IllegalStateException e =
            new IllegalStateException("Cannot find the value for the RequiredImplementation of the " + clazz
            + ". Check that you don't pass null as an implementation value.");
        LOG.log(Level.SEVERE, "Failed to build configuration", e);
        throw e;
      }
    }
    for (final Class<? extends Name<?>> clazz : c.builder.freeParams.keySet()) {
      final Param<?> p = c.builder.freeParams.get(clazz);
      final String s = c.setParams.get(p);
      boolean foundOne = false;
      if (s != null) {
        c.builder.b.bindNamedParameter(clazz, s);
        foundOne = true;
      }
      // Find the bound list for the NamedParameter
      final List list = c.setParamLists.get(p);
      if (list != null) {
        c.builder.b.bindList((Class) clazz, list);
        foundOne = true;
      }
      for (final String paramStr : c.setParamSets.getValuesForKey(p)) {
        c.builder.b.bindSetEntry((Class) clazz, paramStr);
        foundOne = true;
      }

      if (!foundOne && !(p instanceof OptionalParameter)) {
        final IllegalStateException e =
            new IllegalStateException("Cannot find the value for the RequiredParameter of the " + clazz
                    + ". Check that you don't pass null as the parameter value.");
        LOG.log(Level.SEVERE, "Failed to build configuration", e);
        throw e;
      }

    }

    return c.builder.b.build();

  }


  public Set<NamedParameterNode<?>> getBoundNamedParameters() {
    final Configuration c = this.builder.b.build();
    final Set<NamedParameterNode<?>> nps = new MonotonicSet<>();
    nps.addAll(c.getNamedParameters());
    for (final Class<?> np : this.builder.freeParams.keySet()) {
      try {
        nps.add((NamedParameterNode<?>) builder.b.getClassHierarchy().getNode(ReflectionUtilities.getFullName(np)));
      } catch (final NameResolutionException e) {
        throw new IllegalStateException(e);
      }
    }
    return nps;
  }

  /**
   * Replace any \'s in the input string with \\. and any "'s with \".
   *
   * @param in
   * @return
   */
  private static String escape(final String in) {
    // After regexp escaping \\\\ = 1 slash, \\\\\\\\ = 2 slashes.

    // Also, the second args of replaceAll are neither strings nor regexps, and
    // are instead a special DSL used by Matcher. Therefore, we need to double
    // escape slashes (4 slashes) and quotes (3 slashes + ") in those strings.
    // Since we need to write \\ and \", we end up with 8 and 7 slashes,
    // respectively.
    return in.replaceAll("\\\\", "\\\\\\\\").replaceAll("\"", "\\\\\\\"");
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

  /**
   * Convert Configuration to a list of strings formatted as "param=value".
   *
   * @param c
   * @return
   */
  private static List<String> toConfigurationStringList(final Configuration c) {
    final ConfigurationImpl conf = (ConfigurationImpl) c;
    final List<String> l = new ArrayList<>();
    for (final ClassNode<?> opt : conf.getBoundImplementations()) {
      l.add(opt.getFullName()
          + '='
          + escape(conf.getBoundImplementation(opt).getFullName()));
    }
    for (final ClassNode<?> opt : conf.getBoundConstructors()) {
      l.add(opt.getFullName()
          + '='
          + escape(conf.getBoundConstructor(opt).getFullName()));
    }
    for (final NamedParameterNode<?> opt : conf.getNamedParameters()) {
      l.add(opt.getFullName()
          + '='
          + escape(conf.getNamedParameter(opt).toString()));
    }
    for (final ClassNode<?> cn : conf.getLegacyConstructors()) {
      final StringBuilder sb = new StringBuilder();
      join(sb, "-", conf.getLegacyConstructor(cn).getArgs());
      l.add(cn.getFullName()
          + escape('='
              + ConfigurationBuilderImpl.INIT
              + '('
              + sb.toString()
              + ')'
      ));
    }
    for (final NamedParameterNode<Set<?>> key : conf.getBoundSets()) {
      for (final Object value : conf.getBoundSet(key)) {
        final String val;
        if (value instanceof String) {
          val = (String) value;
        } else if (value instanceof Node) {
          val = ((Node) value).getFullName();
        } else {
          throw new IllegalStateException("The value bound to a given NamedParameterNode "
                  + key + " is neither the set of class hierarchy nodes nor strings.");
        }
        l.add(key.getFullName() + '=' + escape(val));
      }
    }
    return l;
  }

  public List<Entry<String, String>> toStringPairs() {
    final List<Entry<String, String>> ret = new ArrayList<>();
    class MyEntry implements Entry<String, String> {
      private final String k;
      private final String v;

      MyEntry(final String k, final String v) {
        this.k = k;
        this.v = v;
      }

      @Override
      public String getKey() {
        return k;
      }

      @Override
      public String getValue() {
        return v;
      }

      @Override
      public String setValue(final String value) {
        throw new UnsupportedOperationException();
      }

    }
    for (final Class<?> c : this.builder.freeParams.keySet()) {
      ret.add(new MyEntry(ReflectionUtilities.getFullName(c),
          this.builder.map.get(this.builder.freeParams.get(c)).getName()));
    }
    for (final Class<?> c : this.builder.freeImpls.keySet()) {
      ret.add(new MyEntry(ReflectionUtilities.getFullName(c),
          this.builder.map.get(this.builder.freeImpls.get(c)).getName()));
    }
    for (final String s : toConfigurationStringList(builder.b.build())) {
      final String[] tok = s.split("=", 2);
      ret.add(new MyEntry(tok[0], tok[1]));
    }

    return ret;
  }

  public String toPrettyString() {
    final StringBuilder sb = new StringBuilder();

    for (final Entry<String, String> l : toStringPairs()) {
      sb.append(l.getKey() + "=" + l.getValue() + "\n");
    }
    return sb.toString();
  }

  public void assertStaticClean() throws ClassHierarchyException {
    if (!(
        setImpls.isEmpty() &&
            setImplSets.isEmpty() &&
            setLateImplSets.isEmpty() &&
            setParamSets.isEmpty() &&
            setLateImpls.isEmpty() &&
            setParams.isEmpty() &&
            setImplLists.isEmpty() &&
            setParamLists.isEmpty()
      )) {
      throw new ClassHierarchyException("Detected statically set ConfigurationModule Parameter / Implementation.  " +
          "set() should only be used dynamically.  Use bind...() instead.");
    }
  }

  public ConfigurationModuleBuilder getBuilder() {
    return builder;
  }
}
