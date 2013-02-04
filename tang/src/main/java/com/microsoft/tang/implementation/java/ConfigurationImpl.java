package com.microsoft.tang.implementation.java;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;

import com.microsoft.tang.ClassNode;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.ConstructorArg;
import com.microsoft.tang.NamedParameterNode;
import com.microsoft.tang.Node;
import com.microsoft.tang.exceptions.NameResolutionException;

public class ConfigurationImpl implements Configuration {
  final ConfigurationBuilderImpl builder;

  ConfigurationImpl(ConfigurationBuilderImpl builder) {
    this.builder = builder;
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

    for (String opt : builder.namespace.getRegisteredClassNames()) {
      try {
        Node n = builder.namespace.getNode(opt);
        if (n instanceof NamedParameterNode) {
          // XXX escaping of strings!!!
          s.append(n.getFullName() + "=" + ConfigurationBuilderImpl.REGISTERED
              + "\n");
        }
      } catch (NameResolutionException e) {
        throw new IllegalStateException("Found partially registered class?", e);
      }
    }
    for (Node opt : builder.boundImpls.keySet()) {
      s.append(opt.getFullName() + "="
          + builder.boundImpls.get(opt).getFullName() + "\n");
    }
    for (Node opt : builder.boundConstructors.keySet()) {
      s.append(opt.getFullName() + "="
          + builder.boundConstructors.get(opt).getFullName() + "\n");
    }
    for (Node opt : builder.namedParameters.keySet()) {
      s.append(opt.getFullName() + "=" + builder.namedParameters.get(opt)
          + "\n");
    }
    for (Node opt : builder.singletons) {
      // ret.put(opt.getFullName(), SINGLETON);
      s.append(opt.getFullName() + "=" + ConfigurationBuilderImpl.SINGLETON
          + "\n");
    }
    for (ClassNode<?> cn : builder.legacyConstructors.keySet()) {
      s.append(cn.getFullName() + "=" + ConfigurationBuilderImpl.INIT + "("
          + join("-", builder.legacyConstructors.get(cn).getArgs()) + ")");
    }
    return s.toString();
  }

  private String join(String sep, ConstructorArg[] types) {
    if (types.length == 0) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    sb.append(types[0].getType());
    for (int i = 1; i < types.length; i++) {
      sb.append(sep + types[i].getType());
    }
    return sb.toString();
  }
}