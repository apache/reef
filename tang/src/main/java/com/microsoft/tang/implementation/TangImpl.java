package com.microsoft.tang.implementation;

import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.ExternalConstructor;
import com.microsoft.tang.JavaClassHierarchy;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Injector;
import com.microsoft.tang.Tang;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.implementation.java.ClassHierarchyImpl;
import com.microsoft.tang.implementation.java.InjectorImpl;
import com.microsoft.tang.implementation.java.JavaConfigurationBuilderImpl;

public class TangImpl implements Tang {

  @Override
  public Injector newInjector(Configuration... confs) throws BindException {
    return new InjectorImpl(new JavaConfigurationBuilderImpl(confs).build());
  }

  @SuppressWarnings("unchecked")
  @Override
  public JavaConfigurationBuilder newConfigurationBuilder() {
    try {
      return newConfigurationBuilder(new URL[0], new Configuration[0], new Class[0]);
    } catch (BindException e) {
      throw new IllegalStateException(
          "Caught unexpeceted bind exception!  Implementation bug.", e);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public JavaConfigurationBuilder newConfigurationBuilder(URL... jars) {
    try {
      return newConfigurationBuilder(jars, new Configuration[0], new Class[0]);
    } catch (BindException e) {
      throw new IllegalStateException(
          "Caught unexpeceted bind exception!  Implementation bug.", e);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public JavaConfigurationBuilder newConfigurationBuilder(
      Configuration... confs) throws BindException {
    return newConfigurationBuilder(new URL[0], confs, new Class[0]);
  }
  @Override
  public final JavaConfigurationBuilder newConfigurationBuilder(
      @SuppressWarnings("unchecked") Class<? extends ExternalConstructor<?>>... parsers) throws BindException {
    return newConfigurationBuilder(new URL[0], new Configuration[0], parsers);
  }

  @Override
  public JavaConfigurationBuilder newConfigurationBuilder(URL[] jars,
      Configuration[] confs, Class<? extends ExternalConstructor<?>>[] parameterParsers) throws BindException {
    JavaConfigurationBuilder cb = new JavaConfigurationBuilderImpl(jars, confs, parameterParsers);
//    for (Configuration c : confs) {
//      cb.addConfiguration(c);
//    }
    return cb;
  }

  private class SetValuedKey {
    public final Set<Object> key;

    public SetValuedKey(Object[] ts, Object[] us) {
      key = new HashSet<Object>(Arrays.asList(ts));
      key.addAll(Arrays.asList(us));
    }
    @Override
    public int hashCode() {
      int i = 0;
      for(Object t : key) {
        i+=t.hashCode();
      }
      return i;
    }
    @Override
    public boolean equals(Object o) {
      SetValuedKey other = (SetValuedKey)o;
      if(other.key.size() != this.key.size()) { return false; }
      return key.containsAll(other.key);
    }
  }
  
  private static Map<SetValuedKey, JavaClassHierarchy> defaultClassHierarchy = new HashMap<>();

  /**
   * Only for testing. Deletes Tang's current database of known classes, forcing
   * it to rebuild them over time.
   * 
   */
  public static void reset() {
   defaultClassHierarchy = new HashMap<>(); //new ClassHierarchyImpl();
  }
  @SuppressWarnings("unchecked")
  @Override
  public JavaClassHierarchy getDefaultClassHierarchy() {
    return getDefaultClassHierarchy(new URL[0], new Class[0]);
  }
  @Override
  public JavaClassHierarchy getDefaultClassHierarchy(URL[] jars, Class<? extends ExternalConstructor<?>>[] parameterParsers) {
    SetValuedKey key = new SetValuedKey(jars, parameterParsers);
    
    JavaClassHierarchy ret = defaultClassHierarchy.get(key);
    if(ret == null) {
      ret = new ClassHierarchyImpl(jars, parameterParsers);
      defaultClassHierarchy.put(key, ret);
    }
    return ret;
  }

}
