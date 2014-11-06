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

import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.ClassHierarchyException;
import org.apache.reef.tang.exceptions.NameResolutionException;
import org.apache.reef.tang.types.NamedParameterNode;
import org.apache.reef.tang.util.*;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Allows applications to bundle sets of configuration options together into
 * discrete packages.  Unlike more conventional approaches,
 * ConfigurationModules store such information in static data structures that
 * can be statically discovered and sanity-checked.
 *
 * @see org.apache.reef.tang.formats.TestConfigurationModule for more information and examples.
 */
public class ConfigurationModule {
  final ConfigurationModuleBuilder builder;
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

  protected ConfigurationModule(ConfigurationModuleBuilder builder) {
    this.builder = builder.deepCopy();
  }

  private ConfigurationModule deepCopy() {
    ConfigurationModule cm = new ConfigurationModule(builder.deepCopy());
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

  private final <T> void processSet(Object impl) {
    Field f = builder.map.get(impl);
    if (f == null) { /* throw */
      throw new ClassHierarchyException("Unknown Impl/Param when setting " + ReflectionUtilities.getSimpleName(impl.getClass()) + ".  Did you pass in a field from some other module?");
    }
    if (!reqSet.contains(f)) {
      reqSet.add(f);
    }
  }

  public final <T> ConfigurationModule set(Impl<T> opt, Class<? extends T> impl) {
    ConfigurationModule c = deepCopy();
    c.processSet(opt);
    if (c.builder.setOpts.contains(opt)) {
      c.setImplSets.put(opt, impl);
    } else {
      c.setImpls.put(opt, impl);
    }
    return c;
  }

  public final <T> ConfigurationModule set(Impl<T> opt, String impl) {
    ConfigurationModule c = deepCopy();
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
   * @param <T>
   * @return
   */
  public final <T> ConfigurationModule set(Impl<List> opt, List implList) {
    ConfigurationModule c = deepCopy();
    c.processSet(opt);
    c.setImplLists.put(opt, implList);
    return c;
  }

  public final <T> ConfigurationModule set(Param<T> opt, Class<? extends T> val) {
    return set(opt, ReflectionUtilities.getFullName(val));
  }

  public final ConfigurationModule set(Param<Boolean> opt, boolean val) {
    return set(opt, "" + val);
  }

  public final ConfigurationModule set(Param<? extends Number> opt, Number val) {
    return set(opt, "" + val);
  }

  public final <T> ConfigurationModule set(Param<T> opt, String val) {
    ConfigurationModule c = deepCopy();
    c.processSet(opt);
    if (c.builder.setOpts.contains(opt)) {
      c.setParamSets.put(opt, val);
    } else {
      c.setParams.put(opt, val);
    }
    return c;
  }

  /**
   * Binds a list to a specfici optional/required Param using ConfigurationModule.
   *
   * @param opt      target optional/required Param
   * @param implList List object to be injected
   * @param <T>
   * @return
   */
  public final <T> ConfigurationModule set(Param<List> opt, List implList) {
    ConfigurationModule c = deepCopy();
    c.processSet(opt);
    c.setParamLists.put(opt, implList);
    return c;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public Configuration build() throws BindException {
    ConfigurationModule c = deepCopy();

    if (!c.reqSet.containsAll(c.builder.reqDecl)) {
      Set<Field> missingSet = new MonotonicHashSet<>();
      for (Field f : c.builder.reqDecl) {
        if (!c.reqSet.contains(f)) {
          missingSet.add(f);
        }
      }
      throw new BindException(
          "Attempt to build configuration before setting required option(s): "
              + builder.toString(missingSet));
    }

    for (Class<?> clazz : c.builder.freeImpls.keySet()) {
      Impl<?> i = c.builder.freeImpls.get(clazz);
      if (c.setImpls.containsKey(i)) {
        c.builder.b.bind(clazz, c.setImpls.get(i));
      } else if (c.setLateImpls.containsKey(i)) {
        c.builder.b.bind(ReflectionUtilities.getFullName(clazz), c.setLateImpls.get(i));
      } else if (c.setImplSets.containsKey(i) || c.setLateImplSets.containsKey(i)) {
        for (Class<?> clz : c.setImplSets.getValuesForKey(i)) {
          c.builder.b.bindSetEntry((Class) clazz, (Class) clz);
        }
        for (String s : c.setLateImplSets.getValuesForKey(i)) {
          c.builder.b.bindSetEntry((Class) clazz, s);
        }
      } else if (c.setImplLists.containsKey(i)) {
        c.builder.b.bindList((Class) clazz, c.setImplLists.get(i));
      }
    }
    for (Class<? extends Name<?>> clazz : c.builder.freeParams.keySet()) {
      Param<?> p = c.builder.freeParams.get(clazz);
      String s = c.setParams.get(p);
      boolean foundOne = false;
      if (s != null) {
        c.builder.b.bindNamedParameter(clazz, s);
        foundOne = true;
      }
      // Find the bound list for the NamedParameter
      List list = c.setParamLists.get(p);
      if (list != null) {
        c.builder.b.bindList((Class) clazz, list);
        foundOne = true;
      }
      for (String paramStr : c.setParamSets.getValuesForKey(p)) {
        c.builder.b.bindSetEntry((Class) clazz, paramStr);
        foundOne = true;
      }
      if (!foundOne) {
        if (!(p instanceof OptionalParameter)) {
          throw new IllegalStateException();
        }
      }
    }
    return c.builder.b.build();

  }

  public Set<NamedParameterNode<?>> getBoundNamedParameters() {
    Configuration c = this.builder.b.build();
    Set<NamedParameterNode<?>> nps = new MonotonicSet<>();
    nps.addAll(c.getNamedParameters());
    for (Class<?> np : this.builder.freeParams.keySet()) {
      try {
        nps.add((NamedParameterNode<?>) builder.b.getClassHierarchy().getNode(ReflectionUtilities.getFullName(np)));
      } catch (NameResolutionException e) {
        throw new IllegalStateException(e);
      }
    }
    return nps;
  }

  public List<Entry<String, String>> toStringPairs() {
    List<Entry<String, String>> ret = new ArrayList<>();
    class MyEntry implements Entry<String, String> {
      final String k;
      final String v;

      public MyEntry(String k, String v) {
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
      public String setValue(String value) {
        throw new UnsupportedOperationException();
      }

    }
    for (Class<?> c : this.builder.freeParams.keySet()) {
      ret.add(new MyEntry(ReflectionUtilities.getFullName(c), this.builder.map.get(this.builder.freeParams.get(c)).getName()));
    }
    for (Class<?> c : this.builder.freeImpls.keySet()) {
      ret.add(new MyEntry(ReflectionUtilities.getFullName(c), this.builder.map.get(this.builder.freeImpls.get(c)).getName()));
    }
    for (String s : ConfigurationFile.toConfigurationStringList(builder.b.build())) {
      String[] tok = s.split("=", 2);
      ret.add(new MyEntry(tok[0], tok[1]));
    }

    return ret;
  }

  public String toPrettyString() {
    StringBuilder sb = new StringBuilder();

    for (Entry<String, String> l : toStringPairs()) {
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
      throw new ClassHierarchyException("Detected statically set ConfigurationModule Parameter / Implementation.  set() should only be used dynamically.  Use bind...() instead.");
    }
  }
}
