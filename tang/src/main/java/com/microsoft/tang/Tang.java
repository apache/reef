package com.microsoft.tang;

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

import com.microsoft.tang.InjectionPlan.Instance;
import com.microsoft.tang.TypeHierarchy.ClassNode;
import com.microsoft.tang.TypeHierarchy.ConstructorArg;
import com.microsoft.tang.TypeHierarchy.ConstructorDef;
import com.microsoft.tang.TypeHierarchy.NamedParameterNode;
import com.microsoft.tang.TypeHierarchy.NamespaceNode;
import com.microsoft.tang.TypeHierarchy.Node;
import com.microsoft.tang.TypeHierarchy.PackageNode;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.exceptions.NameResolutionException;

public class Tang {
  private final TypeHierarchy namespace;
  private final Map<Node, Class<?>> defaultImpls = new HashMap<Node, Class<?>>();
  private final Map<Node, Object> defaultInstances = new HashMap<Node, Object>();

  public Tang(TypeHierarchy namespace) {
    this.namespace = namespace;
    namespace.resolveAllClasses();
  }

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
            namespace.register(Class.forName(value));
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
        // opts.addOption(OptionBuilder.withLongOpt(shortName).hasArg()
        // .withDescription(param.toString()).create());
        opts.addOption(shortName, true, param.toString());
      }
    }
    return opts;
  }

  public void processCommandLine(String[] args) throws NumberFormatException,
      NameResolutionException, ParseException {
    Options o = getCommandLineOptions();
    Option helpFlag = new Option("?", "help");
    o.addOption(helpFlag);
    Parser g = new GnuParser();
    CommandLine cl = g.parse(o, args);
    if (cl.hasOption("?")) {
      HelpFormatter help = new HelpFormatter();
      help.printHelp("reef", o);
      return;
    }
    for (Object ob : o.getOptions()) {
      Option option = (Option) ob;
      String shortName = option.getOpt();
      String value = option.getValue();
      // System.out.println("Got option " + shortName + " = " + value);
      // if(cl.hasOption(shortName)) {
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

  static final InjectionPlan BUILDING = new InjectionPlan() {
    @Override
    public int getNumAlternatives() {
      throw new UnsupportedOperationException();
    }
  };

  private void buildInjectionPlan(String name, Map<String, InjectionPlan> memo)
      throws NameResolutionException {
    if (memo.containsKey(name)) {
      if (BUILDING == memo.get(name)) {
        throw new IllegalStateException("Detected loopy constructor involving "
            + name);
      } else {
        return;
      }
    }
    memo.put(name, BUILDING);

    Node n = namespace.getNode(name);
    final InjectionPlan ip;
    if (n instanceof NamedParameterNode) {
      NamedParameterNode np = (NamedParameterNode) n;
      Object instance = defaultInstances.get(n);
      ip = new Instance(np, instance);
    } else if (n instanceof ClassNode) {
      ClassNode cn = (ClassNode) n;
      if(defaultInstances.containsKey(cn)) {
        ip = new Instance(cn, defaultInstances.get(cn));
      } else if(defaultImpls.containsKey(cn)) {
        String implName = defaultImpls.get(cn).getName();
        buildInjectionPlan(implName, memo);
        ip = memo.get(implName);
      } else {
        List<ClassNode> classNodes = new ArrayList<ClassNode>();
        for(ClassNode c: namespace.getKnownImpls(cn)) {
          classNodes.add(c);
        }
        classNodes.add(cn);
        List<InjectionPlan> sub_ips = new ArrayList<InjectionPlan>();
        for(ClassNode thisCN : classNodes) {
          List<InjectionPlan.Constructor> constructors = new ArrayList<InjectionPlan.Constructor>();
          for (ConstructorDef def : thisCN.injectableConstructors) {
            List<InjectionPlan> args = new ArrayList<InjectionPlan>();
            for (ConstructorArg arg : def.args) {
              String argName = arg.getFullyQualifiedName(thisCN.clazz);
              buildInjectionPlan(argName, memo);
              args.add(memo.get(argName));
            }
            constructors.add(new InjectionPlan.Constructor(def, args
                .toArray(new InjectionPlan[0])));
          }
          sub_ips.add(new InjectionPlan.AmbiguousInjectionPlan(
              constructors.toArray(new InjectionPlan[0])));
        }
        ip = new InjectionPlan.AmbiguousInjectionPlan(sub_ips.toArray(new InjectionPlan[0]));
      }
    } else if (n instanceof PackageNode) {
      throw new IllegalArgumentException(
          "Request to instantiate Java package as object");
    } else if (n instanceof NamespaceNode) {
      throw new IllegalArgumentException(
          "Request to instantiate Tang namespace as object");
    } else {
      throw new IllegalStateException(
          "Type hierarchy contained unknown node type!:" + n);
    }
    memo.put(name, ip);
  }

  public InjectionPlan getInjectionPlan(String name)
      throws NameResolutionException {
    Map<String, InjectionPlan> memo = new HashMap<String, InjectionPlan>();
    buildInjectionPlan(name, memo);
    return memo.get(name);
  }

  public boolean canInject(String name) throws NameResolutionException {
    InjectionPlan p = getInjectionPlan(name);
    boolean ret = p.getNumAlternatives() == 1;
    boolean oldret = canInjectOld(name);
    if (ret != oldret) {
      throw new IllegalStateException(
          "Found bug in old or new implementation of canInject().  Name is "
              + name + " old says " + oldret + " new says " + ret);
    }
    return ret;
  }

  public boolean canInjectOld(String name) { // throws NameResolutionException {
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
        return canInjectOld(clz.getName());
      }
      ClassNode[] cn = namespace.getKnownImpls(c);
      boolean haveAltImpl = false;
      if (cn.length == 1) {
        if (canInjectOld(cn[0].clazz.getName())) {
          haveAltImpl = true;
        }
      } else if (cn.length == 0) {
        // we don't consider something to be an impl of itself
        // so fall through and check this class.
      }
      for (ConstructorDef def : c.injectableConstructors) {
        boolean canInject = true;
        for (ConstructorArg arg : def.args) {
          if (!canInjectOld(arg.getFullyQualifiedName(c.clazz))) {
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
