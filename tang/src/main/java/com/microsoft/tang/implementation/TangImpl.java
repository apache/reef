package com.microsoft.tang.implementation;

import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;
import com.microsoft.tang.Configuration;
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

  @Override
  public JavaConfigurationBuilder newConfigurationBuilder() {
    try {
      return newConfigurationBuilder(new URL[0], new Configuration[0]);
    } catch (BindException e) {
      throw new IllegalStateException(
          "Caught unexpeceted bind exception!  Implementation bug.", e);
    }
  }

  @Override
  public JavaConfigurationBuilder newConfigurationBuilder(URL... jars) {
    try {
      return newConfigurationBuilder(jars, new Configuration[0]);
    } catch (BindException e) {
      throw new IllegalStateException(
          "Caught unexpeceted bind exception!  Implementation bug.", e);
    }
  }

  @Override
  public JavaConfigurationBuilder newConfigurationBuilder(
      Configuration... confs) throws BindException {
    return newConfigurationBuilder(new URL[0], confs);

  }

  @Override
  public JavaConfigurationBuilder newConfigurationBuilder(URL[] jars,
      Configuration[] confs) throws BindException {
    JavaConfigurationBuilder cb = new JavaConfigurationBuilderImpl(jars);
    for (Configuration c : confs) {
      cb.addConfiguration(c);
    }
    return cb;
  }

  private class SetValuedKey<T> {
    public final Set<T> key;
    @SafeVarargs
    public SetValuedKey(T...ts) {
      key = new HashSet<>(Arrays.asList(ts));
    }
    @Override
    public int hashCode() {
      int i = 0;
      for(T t : key) {
        i+=t.hashCode();
      }
      return i;
    }
    @Override
    public boolean equals(Object o) {
      @SuppressWarnings("unchecked")
      SetValuedKey<T> other = (SetValuedKey<T>)o;
      if(other.key.size() != this.key.size()) { return false; }
      return key.containsAll(other.key);
    }
  }
  
  private static Map<SetValuedKey<URL>, JavaClassHierarchy> defaultClassHierarchy = new HashMap<>();

  /**
   * Only for testing. Deletes Tang's current database of known classes, forcing
   * it to rebuild them over time.
   * 
   */
  public static void reset() {
   defaultClassHierarchy = new HashMap<>(); //new ClassHierarchyImpl();
  }

  @Override
  public JavaClassHierarchy getDefaultClassHierarchy(URL... jars) {
    SetValuedKey<URL> key = new SetValuedKey<>(jars);
    
    JavaClassHierarchy ret = defaultClassHierarchy.get(key);
    if(ret == null) {
      ret = new ClassHierarchyImpl(jars);
      defaultClassHierarchy.put(key, ret);
    }
    return ret;
  }

}
