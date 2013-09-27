package com.microsoft.tang.formats;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.microsoft.tang.Configuration;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.ClassHierarchyException;
import com.microsoft.tang.exceptions.NameResolutionException;
import com.microsoft.tang.formats.Impl;
import com.microsoft.tang.formats.OptionalParameter;
import com.microsoft.tang.formats.Param;
import com.microsoft.tang.types.NamedParameterNode;
import com.microsoft.tang.util.MonotonicHashMap;
import com.microsoft.tang.util.MonotonicHashSet;
import com.microsoft.tang.util.MonotonicMultiHashMap;
import com.microsoft.tang.util.MonotonicSet;
import com.microsoft.tang.util.ReflectionUtilities;

/**
 * Allows applications to bundle sets of configuration options together into 
 * discrete packages.  Unlike more conventional approaches,
 * ConfigurationModules store such information in static data structures that
 * can be statically discovered and sanity-checked. 
 * 
 * @see com.microsoft.tang.formats.TestConfigurationModule for more information and examples.
 *
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
  protected ConfigurationModule(ConfigurationModuleBuilder builder) {
    this.builder = builder.deepCopy();
  }
  private ConfigurationModule deepCopy() {
    ConfigurationModule cm = new ConfigurationModule(builder.deepCopy());
    cm.setImpls.putAll(setImpls);
    cm.setImplSets.addAll(setImplSets);
    cm.setParamSets.addAll(setParamSets);
    cm.setLateImpls.putAll(setLateImpls);
    cm.setParams.putAll(setParams);
    cm.reqSet.addAll(reqSet);
    return cm;
  }

  private final <T> void processSet(Object impl) {
    Field f = builder.map.get(impl);
    if (f == null) { /* throw */
      throw new ClassHierarchyException("Unknown Impl/Param when setting " + ReflectionUtilities.getSimpleName(impl.getClass()) + ".  Did you pass in a field from some other module?");
    }
    if(!reqSet.contains(f)) { reqSet.add(f); }
  }

  public final <T> ConfigurationModule set(Impl<T> opt, Class<? extends T> impl) {
    ConfigurationModule c = deepCopy();
    c.processSet(opt);
    if(c.builder.setOpts.contains(opt)) {
      c.setImplSets.put(opt, impl);
    } else {
      c.setImpls.put(opt, impl);
    }
    return c;
  }

  public final <T> ConfigurationModule set(Impl<T> opt, String impl) {
    ConfigurationModule c = deepCopy();
    c.processSet(opt);
    if(c.builder.setOpts.contains(opt)) {
      c.setLateImplSets.put(opt, impl);
    } else {
      c.setLateImpls.put(opt, impl);
    }
    return c;
  }
  
  public final <T> ConfigurationModule set(Param<T> opt, Class<? extends T> val) {
    return set(opt, ReflectionUtilities.getFullName(val));
  }
  public final <T> ConfigurationModule set(Param<T> opt, Number val) {
    return set(opt, ""+val);
  }
  public final <T> ConfigurationModule set(Param<T> opt, String val) {
    ConfigurationModule c = deepCopy();
    c.processSet(opt);
    if(c.builder.setOpts.contains(opt)) {
      c.setParamSets.put(opt, val);
    } else {
      c.setParams.put(opt, val);
    }
    return c;
  }
  
/*  public final InjectorModule buildVolatileInjector() throws ClassHierarchyException {
    ConfigurationModule c = deepCopy();

    return new InjectorModule(c);  // injector module will all bind volatiles over missing sets in c.
    
    // it basically just delegates to an unsafe version of build, gets an injector, and at build() bind volatiles into injector
  } */

  
  @SuppressWarnings({ "unchecked", "rawtypes" })
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
      if(c.setImpls.containsKey(i)) {
        c.builder.b.bind(clazz, c.setImpls.get(i));
      } else if(c.setLateImpls.containsKey(i)) {
        c.builder.b.bind(ReflectionUtilities.getFullName(clazz), c.setLateImpls.get(i));
      } else {
        for(Class<?> clz : c.setImplSets.getValuesForKey(i)) {
          c.builder.b.bindSetEntry((Class)clazz, (Class)clz);
        }
        for(String s : c.setLateImplSets.getValuesForKey(i)) {
          c.builder.b.bindSetEntry((Class)clazz, s);
        }
      }
    }
    for (Class<? extends Name<?>> clazz : c.builder.freeParams.keySet()) {
      Param<?> p = c.builder.freeParams.get(clazz);
      String s = c.setParams.get(p);
      boolean foundOne = false;
      if(s != null) {
        c.builder.b.bindNamedParameter(clazz, s);
        foundOne = true;
      }
      for(String paramStr : c.setParamSets.getValuesForKey(p)) {
        c.builder.b.bindSetEntry((Class)clazz, paramStr);
        foundOne = true;
      }
      if(! foundOne) {
        if(!(p instanceof OptionalParameter)) {
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
    for(Class<?> np : this.builder.freeParams.keySet()) {
      try {
        nps.add((NamedParameterNode<?>)builder.b.getClassHierarchy().getNode(ReflectionUtilities.getFullName(np)));
      } catch (NameResolutionException e) {
        throw new IllegalStateException(e);
      }
    }
    return nps;
  }
  public List<Entry<String,String>> toStringPairs() {
    List<Entry<String,String>> ret = new ArrayList<>();
    class MyEntry implements Entry<String,String>{
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
    for(Class<?> c : this.builder.freeParams.keySet()) {
      ret.add(new MyEntry(ReflectionUtilities.getFullName(c), this.builder.map.get(this.builder.freeParams.get(c)).getName()));
    }
    for(Class<?> c : this.builder.freeImpls.keySet()) {
      ret.add(new MyEntry(ReflectionUtilities.getFullName(c), this.builder.map.get(this.builder.freeImpls.get(c)).getName()));
    }
    
    return ret;
  }
  public String toPrettyString() {
    StringBuilder sb = new StringBuilder();
    
    for(Entry<String,String> l : toStringPairs()) {
      sb.append(l.getKey() + "=" + l.getValue() + "\n");
    }
    sb.append(ConfigurationFile.toConfigurationString(builder.b.build()));
    return sb.toString();
  }
}
