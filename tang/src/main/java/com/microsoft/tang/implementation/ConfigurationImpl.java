package com.microsoft.tang.implementation;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.microsoft.tang.Configuration;
import com.microsoft.tang.ExternalConstructor;
import com.microsoft.tang.exceptions.NameResolutionException;
import com.microsoft.tang.implementation.TypeHierarchy.ClassNode;
import com.microsoft.tang.implementation.TypeHierarchy.NamedParameterNode;
import com.microsoft.tang.implementation.TypeHierarchy.Node;
import com.microsoft.tang.util.MonotonicMap;
import com.microsoft.tang.util.MonotonicSet;

public class ConfigurationImpl implements Configuration {
  final TypeHierarchy namespace = new TypeHierarchy();
  final Map<ClassNode<?>, Class<?>> boundImpls = new MonotonicMap<ClassNode<?>, Class<?>>();
  final Map<ClassNode<?>, Class<ExternalConstructor<?>>> boundConstructors = new MonotonicMap<ClassNode<?>, Class<ExternalConstructor<?>>>();
  final Set<ClassNode<?>> singletons = new MonotonicSet<ClassNode<?>>();
  final Map<NamedParameterNode<?>, String> namedParameters = new MonotonicMap<NamedParameterNode<?>, String>();

  // *Not* serialized.
  final Map<ClassNode<?>, Object> singletonInstances = new MonotonicMap<ClassNode<?>, Object>();
  final Map<NamedParameterNode<?>, Object> namedParameterInstances = new MonotonicMap<NamedParameterNode<?>, Object>();
  private final List<URL> jars;
  private URLClassLoader loader;

  public URL[] getJars() {
    return jars.toArray(new URL[0]);
  }

  Class<?> classForName(String name) throws ClassNotFoundException {
    return loader.loadClass(name);
  }

  boolean sealed = false;
  boolean dirtyBit = false;

  public final static String IMPORT = "import";
  public final static String REGISTERED = "registered";
  public final static String SINGLETON = "singleton";

  public ConfigurationImpl(URL... jars) {
    this.jars = new ArrayList<>(Arrays.asList(jars));
    this.loader = new URLClassLoader(jars, this.getClass().getClassLoader());
  }

  public ConfigurationImpl(ClassLoader loader, URL... jars) {
    this.jars = new ArrayList<URL>(Arrays.asList(jars));
    this.loader = new URLClassLoader(jars, loader);
  }

  public void addJars(URL... j) {
    List<URL> newJars = new ArrayList<>();
    for (URL u : j) {
      if (!this.jars.contains(u)) {
        newJars.add(u);
        this.jars.add(u);
      }
    }
    // Note, URL class loader first looks in its parent, then in the array of
    // URLS passed in, in order. So, this line is equivalent to "reaching into"
    // URLClassLoader and adding the URLS to the end of the array.
    this.loader = new URLClassLoader(newJars.toArray(new URL[0]), this.loader);
  }

  @Override
  public void writeConfigurationFile(File f) throws IOException {
    OutputStream o = new FileOutputStream(f);
    writeConfigurationFile(o);
    o.close();
  }
  @Override
  public void writeConfigurationFile(OutputStream o) {
    PrintStream p = new PrintStream(o);
    p.print(getConfigurationString());
    p.flush();
  }
  @Override
  public String getConfigurationString() {
    StringBuilder s = new StringBuilder();

    if (dirtyBit) {
      throw new IllegalStateException(
          "Someone called setVolatileInstance() on this ConfigurationBuilderImpl object.  Refusing to serialize it!");
    }
    
    for (Class<?> opt : namespace.getRegisteredClasses()) {
      try {
        Node n = namespace.getNode(opt);
        if(n instanceof NamedParameterNode) {
          // XXX escaping of strings!!!
          s.append(n.getFullName() + "=" + REGISTERED + "\n");
        }
      } catch (NameResolutionException e) {
        throw new IllegalStateException("Found partially registered class?", e);
      }
    }
    for (Node opt : boundImpls.keySet()) {
      s.append(opt.getFullName() + "=" + boundImpls.get(opt).getName() + "\n");
    }
    for (Node opt : boundConstructors.keySet()) {
      s.append(opt.getFullName() + "=" + boundConstructors.get(opt).getName() + "\n");
    }
    for (Node opt : namedParameters.keySet()) {
      s.append(opt.getFullName() + "=" + namedParameters.get(opt) + "\n");
    }
    for (Node opt : singletons) {
      //ret.put(opt.getFullName(), SINGLETON);
      s.append(opt.getFullName() + "=" + SINGLETON + "\n");
    }
    return s.toString();
  }

  /**
   * Obtain the effective configuration of this ConfigurationBuilderImpl
   * instance. This consists of string-string pairs that could be dumped
   * directly to a Properties file, for example. Currently, this method does not
   * return information about default parameter values that were specified by
   * parameter annotations, or about the auto-discovered stuff in TypeHierarchy.
   * All of that should be automatically imported as these keys are parsed on
   * the other end.
   * 
   * @return a String to String map
   */
  private Map<String, String> getConfiguration() {
    if (dirtyBit) {
      throw new IllegalStateException(
          "Someone called setVolatileInstance() on this ConfigurationBuilderImpl object; no introspection allowed!");
    }

    Map<String, String> ret = new HashMap<String, String>();
    for (Class<?> opt : namespace.getRegisteredClasses()) {
      try {
        Node n = namespace.getNode(opt);
        if(n instanceof NamedParameterNode) {
          ret.put(opt.getName(), REGISTERED);
        }
      } catch (NameResolutionException e) {
        throw new IllegalStateException("Found partially registered class?", e);
      }
    }
    for (Node opt : boundImpls.keySet()) {
      ret.put(opt.getFullName(), boundImpls.get(opt).getName());
    }
    for (Node opt : boundConstructors.keySet()) {
      ret.put(opt.getFullName(), boundConstructors.get(opt).getName());
    }
    for (Node opt : namedParameters.keySet()) {
      ret.put(opt.getFullName(), namedParameters.get(opt));
    }
    for (Node opt : singletons) {
      ret.put(opt.getFullName(), SINGLETON);
    }
    return ret;
  }
}