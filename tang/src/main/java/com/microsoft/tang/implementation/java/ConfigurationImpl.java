package com.microsoft.tang.implementation.java;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URL;
import java.util.Map;
import java.util.Set;

import com.microsoft.tang.ClassNode;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.ConstructorArg;
import com.microsoft.tang.ConstructorDef;
import com.microsoft.tang.ExternalConstructor;
import com.microsoft.tang.NamedParameterNode;
import com.microsoft.tang.Node;
import com.microsoft.tang.exceptions.NameResolutionException;
import com.microsoft.tang.util.MonotonicMap;
import com.microsoft.tang.util.MonotonicSet;

public class ConfigurationImpl implements Configuration {
  // TODO: None of these should be public!
  final ClassHierarchyImpl namespace;
  // TODO: Change to Map<ClassNode, ClassNode>
  final Map<ClassNode<?>, ClassNode<?>> boundImpls = new MonotonicMap<>();
  final Map<ClassNode<?>, Class<ExternalConstructor<?>>> boundConstructors = new MonotonicMap<>();
  final Set<ClassNode<?>> singletons = new MonotonicSet<>();
  final Map<NamedParameterNode<?>, String> namedParameters = new MonotonicMap<>();
  final Map<ClassNode<?>, ConstructorDef<?>> legacyConstructors = new MonotonicMap<>();
  
  // *Not* serialized.
  final Map<ClassNode<?>, Object> singletonInstances = new MonotonicMap<>();
  final Map<NamedParameterNode<?>, Object> namedParameterInstances = new MonotonicMap<>();

  boolean sealed = false;
  boolean dirtyBit = false;

  public final static String IMPORT = "import";
  public final static String REGISTERED = "registered";
  public final static String SINGLETON = "singleton";
  public final static String INIT = "<init>";

  public ConfigurationImpl(URL... jars) {
    this.namespace = new ClassHierarchyImpl(jars);
  }

  public ConfigurationImpl(ClassLoader loader, URL... jars) {
    this.namespace = new ClassHierarchyImpl(loader, jars);
  }

//  @Deprecated
//  public void addJars(URL... j) {
//    this.namespace.addJars(j);
//  }
//  @Deprecated
//  public URL[] getJars() {
//    return this.namespace.getJars();
//  }

  
  @Override
  public void writeConfigurationFile(File f) throws IOException {
    OutputStream o = new FileOutputStream(f);
    writeConfigurationFile(o);
    o.close();
  }

  @Override
  public void writeConfigurationFile(OutputStream o) {
    PrintStream p = new PrintStream(o);
    p.print(toConfigurationString());
    p.flush();
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
   *         configuration object (assuming the same classes / jars are
   *         available when the string is parsed by Tang).
   */
  @Override
  public String toConfigurationString() {
    StringBuilder s = new StringBuilder();

    if (dirtyBit) {
      throw new IllegalStateException(
          "Someone called setVolatileInstance() on this ConfigurationBuilderImpl object.  Refusing to serialize it!");
    }

    for (String opt : namespace.getRegisteredClassNames()) {
      try {
        Node n = namespace.getNode(opt);
        if (n instanceof NamedParameterNode) {
          // XXX escaping of strings!!!
          s.append(n.getFullName() + "=" + REGISTERED + "\n");
        }
      } catch (NameResolutionException e) {
        throw new IllegalStateException("Found partially registered class?", e);
      }
    }
    for (Node opt : boundImpls.keySet()) {
      s.append(opt.getFullName() + "=" + boundImpls.get(opt).getFullName() + "\n");
    }
    for (Node opt : boundConstructors.keySet()) {
      s.append(opt.getFullName() + "=" + boundConstructors.get(opt).getName()
          + "\n");
    }
    for (Node opt : namedParameters.keySet()) {
      s.append(opt.getFullName() + "=" + namedParameters.get(opt) + "\n");
    }
    for (Node opt : singletons) {
      // ret.put(opt.getFullName(), SINGLETON);
      s.append(opt.getFullName() + "=" + SINGLETON + "\n");
    }
    for (ClassNode<?> cn : legacyConstructors.keySet()) {
      s.append(cn.getFullName() + "=" + INIT + "(" + join("-", legacyConstructors.get(cn).getArgs()) + ")");
    }
    return s.toString();
  }
  private String join(String sep, ConstructorArg[] types) {
    if(types.length == 0) { return ""; }
    StringBuilder sb = new StringBuilder();
    sb.append(types[0].getType());
    for(int i = 1; i < types.length; i++) {
      sb.append(sep + types[i].getType());
    }
    return sb.toString();
  }
}