package com.microsoft.inject;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import com.microsoft.inject.TypeHierarchy.ClassNode;
import com.microsoft.inject.TypeHierarchy.ConstructorArg;
import com.microsoft.inject.TypeHierarchy.ConstructorDef;
import com.microsoft.inject.TypeHierarchy.NamedParameterNode;
import com.microsoft.inject.TypeHierarchy.Node;
import com.microsoft.inject.annotations.Name;
import com.microsoft.inject.exceptions.NameResolutionException;

public class Tang {
  private final TypeHierarchy namespace;
  private final Map<Node, Class<?>> defaultImpls = new HashMap<Node, Class<?>>();
  private final Map<Node, Object> defaultInstances = new HashMap<Node, Object>();

  public Tang(TypeHierarchy namespace) {
    this.namespace = namespace;
    namespace.resolveAllClasses();
  }

  /*
   * public Tang(Configuration conf) { this.conf = conf; this.namespace = new
   * TypeHierarchy();
   * 
   * Iterator<String> it = this.conf.getKeys();
   * 
   * while (it.hasNext()) { String key = it.next(); String value =
   * this.conf.getString(key);
   * 
   * if (key.equals("tang.import")) { try {
   * namespace.registerClass(Class.forName(value)); } catch
   * (ClassNotFoundException e) { // print error message + exit. } }
   * 
   * } }
   */

  @SuppressWarnings("unchecked")
  public void registerConfigFile(String configFileName)
      throws ConfigurationException, NameResolutionException {
    Configuration conf = new PropertiesConfiguration(configFileName);
    Iterator<String> it = conf.getKeys();

    while (it.hasNext()) {
      String key = it.next();
      String[] values = conf.getStringArray(key);
      for (String value : values) {
        if (key.equals("tang.import")) {
          try {
            namespace.registerClass(Class.forName(value));
            namespace.resolveAllClasses();
          } catch (ClassNotFoundException e) {
            // print error message + exit.
          }
        } else {
          Node n = namespace.getNode(key);
          if (n instanceof NamedParameterNode) {
            NamedParameterNode np = (NamedParameterNode) n;
            setNamedParameter((Class<? extends Name>) np.clazz, new Integer(
                Integer.parseInt(value))); // XXX blatant hack.
          }
        }
      }
    }
    namespace.resolveAllClasses();
  }

  public Options getCommandLineOptions() {
    Options opts = new Options();
    Collection<NamedParameterNode> namedParameters = namespace
        .getNamedParameterNodes();
    for (NamedParameterNode param : namedParameters) {
      String shortName = param.getShortName();
      if (shortName != null) {
//        opts.addOption(OptionBuilder.withLongOpt(shortName).hasArg()
//            .withDescription(param.toString()).create());
        opts.addOption(shortName, true, param.toString());
      }
    }
    return opts;
  }

  public void processCommandLine(String [] args)
      throws NumberFormatException, NameResolutionException, ParseException {
    Options o = getCommandLineOptions();
    Option helpFlag = new Option("?", "help");
    o.addOption(helpFlag);
    Parser g = new GnuParser();
    CommandLine cl = g.parse(o, args);
    if(cl.hasOption("?")) {
      HelpFormatter help = new HelpFormatter();
      help.printHelp("reef", o);
      return;
    }
    for (Object ob : o.getOptions()) {
      Option option = (Option) ob;
      String shortName = option.getOpt();
      String value = option.getValue();
      //System.out.println("Got option " + shortName + " = " + value);
      //if(cl.hasOption(shortName)) {
      NamedParameterNode n = namespace.getNodeFromShortName(shortName);
      if (n != null && value != null) {
        setNamedParameter((Class<? extends Name>) (n.clazz), new Integer(
            Integer.parseInt(value))); // XXX blatant hack
      }
    }
  }

  public Tang() {
    namespace = new TypeHierarchy();
  }

  public void setDefaultImpl(Class<?> c, Class<?> d)
      throws NameResolutionException {
    if (!c.isAssignableFrom(d)) {
      throw new ClassCastException(d.getName()
          + " does not extend or implement " + c.getName());
    }
    Node n = namespace.getNode(c);
    if (n instanceof ClassNode && !(n instanceof NamedParameterNode)) {
      defaultImpls.put(n, d);
    } else {
      // TODO need new exception type here.
      throw new IllegalArgumentException(
          "Detected type mismatch.  Expected ClassNode, but namespace contains a "
              + n);
    }
  }

  public void setNamedParameter(Class<? extends Name> name, Object o)
      throws NameResolutionException {
    Node n = namespace.getNode(name.getName());
    if (n instanceof NamedParameterNode) {
      setNamedParameter((NamedParameterNode) n, o);
    } else {
      // TODO add support for setting default *instance* of class.
      // TODO need new exception type here.
      throw new IllegalArgumentException(
          "Detected type mismatch when setting named parameter " + name
              + "  Expected NamedParameterNode, but namespace contains a " + n);
    }
  }

  public void setNamedParameter(NamedParameterNode np, Object o) {
    if (ReflectionUtilities.isCoercable(np.argClass, o.getClass())) {
      defaultInstances.put(np, o);
    } else {
      throw new ClassCastException("Cannot cast from " + o.getClass() + " to "
          + np.argClass);
    }
  }

  public boolean canInject(String name) { // throws NameResolutionException {
    Node n;
    try {
      n = namespace.getNode(name);
    } catch (NameResolutionException e) {
      e.printStackTrace();
      return false;
    }
    if (n instanceof NamedParameterNode) {
      NamedParameterNode np = (NamedParameterNode) n;
      return defaultInstances.get(np) != null;
    } else if (n instanceof ClassNode) {
      ClassNode c = (ClassNode) n;
      Object instance = defaultInstances.get(c);
      if (instance != null) {
        return true;
      }
      Class<?> clz = defaultImpls.get(c);
      if (clz != null) {
        return canInject(clz.getName());
      }
      ClassNode[] cn = namespace.getKnownImpls(c);
      boolean haveAltImpl = false;
      if (cn.length == 1) {
        if (canInject(cn[0].clazz.getName())) {
          haveAltImpl = true;
        }
      } else if (cn.length == 0) {
        // we don't consider something to be an impl of itself
        // so fall through and check this class.
      }
      for (ConstructorDef def : c.injectableConstructors) {
        boolean canInject = true;
        for (ConstructorArg arg : def.args) {
          if (!canInject(arg.getFullyQualifiedName(c.clazz))) {
            canInject = false;
            break;
          }
        }
        if (canInject) {
          if (haveAltImpl) {
            throw new IllegalStateException("Can't inject due to two impls!"
                + name);
          }
          return true;
        }
      }
      if (haveAltImpl) {
        return true;
      }
      throw new IllegalStateException("Can't inject: " + name);
      // return false;
    } else {
      throw new IllegalArgumentException();
    }
  }

  @SuppressWarnings("unchecked")
  public <U> U getInstance(Class<U> clazz) throws NameResolutionException,
      ReflectiveOperationException {
    Node n = namespace.getNode(clazz);
    if (n instanceof ClassNode && !(n instanceof NamedParameterNode)) {
      Class<U> c = (Class<U>) defaultImpls.get(n);
      if (c != null) {
        return getInstance(c);
      }
      ClassNode[] known = namespace.getKnownImpls((ClassNode) n);
      if (known.length == 1) {
        return getInstance((Class<U>) known[0].clazz);
      } else if (known.length > 1) {
        throw new IllegalStateException("Ambiguous inject of " + clazz
            + " detected.  No default, and known impls are "
            + Arrays.toString(known));
      }
    } else {
      // TODO need new exception type here.
      throw new IllegalStateException("Expected ClassNode, got: "
          + n.toString() + " (" + n.getClass() + ")");
    }
    // OK, n is a ClassNode, and has not been overridden with a call
    // to setDefaultImpl. Let's try to construct it!
    List<ConstructorDef> defs = new ArrayList<ConstructorDef>();
    for (ConstructorDef def : ((ClassNode) n).injectableConstructors) {
      boolean canInject = true;
      for (ConstructorArg arg : def.args) {
        String name = arg.getFullyQualifiedName(clazz);
        if (!canInject(name)) {
          canInject = false;
        }
      }
      if (canInject) {
        defs.add(def);
      }
    }

    // Now, find most specific def, or throw exception for non-comparable defs.
    for (int i = 0; i < defs.size(); i++) {
      for (int j = 0; j < defs.size(); j++) {
        if (defs.get(i).isMoreSpecificThan(defs.get(j))) {
          defs.remove(j);
          if (i >= j) {
            i--;
          }
          j--;
        }
      }
    }
    if (defs.size() == 0) {
      throw new IllegalArgumentException("No injectable constructors for "
          + clazz);
    }
    if (defs.size() > 1) {
      throw new IllegalArgumentException("Ambiguous injection of " + clazz
          + " Found the following constructors: "
          + Arrays.toString(defs.toArray()));
    }
    List<Object> args = new ArrayList<Object>();
    for (ConstructorArg arg : defs.get(0).args) {
      Node argNode = namespace.getNode(arg.getFullyQualifiedName(clazz));
      if (argNode instanceof NamedParameterNode) {
        args.add(defaultInstances.get(argNode));
      } else if (argNode instanceof ClassNode) {
        args.add(getInstance(((ClassNode) argNode).clazz));
      } else {
        throw new IllegalStateException(
            "Expected ClassNode or NamedParameterNode, but got " + argNode);
      }
    }
    try {
      return ((Constructor<U>) (defs.get(0).constructor)).newInstance(args
          .toArray());
    } catch (IllegalArgumentException e) {
      throw new IllegalStateException("Could not invoke constructor "
          + defs.get(0).constructor + " with args "
          + Arrays.toString(args.toArray()) + ": " + e.getMessage(), e);
    }
  }
}
