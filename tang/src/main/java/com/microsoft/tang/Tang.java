package com.microsoft.tang;

import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

import com.microsoft.tang.InjectionPlan.AmbiguousInjectionPlan;
import com.microsoft.tang.InjectionPlan.InfeasibleInjectionPlan;
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
  private final Map<ClassNode<?>, Class<?>> defaultImpls = new MonotonicMap<ClassNode<?>, Class<?>>();
  private final Map<Node, Class<ExternalConstructor<?>>> constructors = new MonotonicMap<Node, Class<ExternalConstructor<?>>>();
  private final Set<ClassNode<?>> singletons = new MonotonicSet<ClassNode<?>>();
  private final Map<NamedParameterNode<?>, String> namedParameters = new MonotonicMap<NamedParameterNode<?>, String>();

  // *Not* serialized.
  private final Map<ClassNode<?>, Object> singletonInstances = new MonotonicMap<ClassNode<?>, Object>();
  private final Map<NamedParameterNode<?>, Object> namedParameterInstances = new MonotonicMap<NamedParameterNode<?>, Object>();
  private boolean sealed = false;

  public Tang() {
    namespace = new TypeHierarchy();
  }

  public Tang(TypeHierarchy namespace) {
    this.namespace = namespace;
  }

  public void register(Class<?> c) {
    namespace.register(c);
  }

  public <T> void processConfigurationFile(File configFileName)
      throws ConfigurationException, NameResolutionException,
      ReflectiveOperationException {
    Configuration conf = new PropertiesConfiguration(configFileName);
    Iterator<String> it = conf.getKeys();

    Map<String, String> shortNames = new HashMap<String, String>();

    while (it.hasNext()) {
      String key = it.next();
      String longName = shortNames.get(key);
      String[] values = conf.getStringArray(key);
      if (longName != null) {
        // System.err.println("Mapped " + key + " to " + longName);
        key = longName;
      }
      for (String value : values) {
        boolean isSingleton = false;
        if(value.equals("tang.singleton")) {
          isSingleton = true;
        }
        if (key.equals("import")) {
          if(isSingleton) {
            throw new IllegalArgumentException("Can't import=tang.singleton.  Makes no sense");
          }
          try {
            namespace.register(Class.forName(value));
            String[] tok = value.split(TypeHierarchy.regexp);
            try {
              namespace.getNode(tok[tok.length - 1]);
              throw new IllegalArgumentException("Conflict on short name: "
                  + tok[tok.length - 1]);
            } catch (NameResolutionException e) {
              String oldValue = shortNames.put(tok[tok.length - 1], value);
              if (oldValue != null) {
                throw new IllegalArgumentException("Name conflict.  "
                    + tok[tok.length - 1] + " maps to " + oldValue + " and "
                    + value);
              }
              // System.err.println("Added mapping from " + tok[tok.length-1] +
              // " to " + value);
            }
          } catch (ClassNotFoundException e) {
            // print error message + exit.
          }
        } else {
          if(isSingleton) {
            bindSingleton(Class.forName(key));
          } else {
            bind(key, value);
          }
        }
      }
    }
  }

  public void writeConfigurationFile(PrintStream s) {
    Map<String, String> effectiveConfiguration = getEffectiveConfiguration();
    for(String k : effectiveConfiguration.keySet()) {
      // XXX escaping of strings!!!
      s.println(k+"="+effectiveConfiguration.get(k));
    }
  }

  private Options getCommandLineOptions() {
    Options opts = new Options();
    Collection<NamedParameterNode<?>> namedParameters = namespace
        .getNamedParameterNodes();
    for (NamedParameterNode<?> param : namedParameters) {
      String shortName = param.getShortName();
      if (shortName != null) {
        // opts.addOption(OptionBuilder.withLongOpt(shortName).hasArg()
        // .withDescription(param.toString()).create());
        opts.addOption(shortName, true, param.toString());
      }
    }
    for (Option o : applicationOptions.keySet()) {
      opts.addOption(o);
    }
    return opts;
  }

  public interface CommandLineCallback {
    public void process(Option option);
  }

  Map<Option, CommandLineCallback> applicationOptions = new HashMap<Option, CommandLineCallback>();

  public void addCommandLineOption(Option option, CommandLineCallback cb) {
    // TODO: Check for conflicting options.
    applicationOptions.put(option, cb);
  }

  public <T> void processCommandLine(String[] args)
      throws NumberFormatException, NameResolutionException, ParseException {
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

      NamedParameterNode<T> n = namespace.getNodeFromShortName(shortName);
      if (n != null && value != null) {
        // XXX completely untested.

        if (applicationOptions.containsKey(option)) {
          applicationOptions.get(option).process(option);
        } else {
          bindParameter(n.clazz, value);
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  public <T> void bind(String key, String value) throws NameResolutionException, ClassNotFoundException {
    if(sealed) throw new IllegalStateException("Can't bind to sealed Tang!");
    Node n = namespace.getNode(key);
    /*String longVal = shortNames.get(value);
    if (longVal != null)
      value = longVal; */
    if (n instanceof NamedParameterNode) {
      bindParameter((NamedParameterNode<?>)n, value);
    } else if (n instanceof ClassNode) {
      Class<T> c = ((ClassNode<T>)n).getClazz();
      Class<?> val = (Class<?>)Class.forName(value);
      if(ExternalConstructor.class.isAssignableFrom(val) && 
          (!ExternalConstructor.class.isAssignableFrom(c))) {
        bindConstructor(c,
            (Class<? extends ExternalConstructor<? extends T>>)val);
      } else {
        bindImplementation(c,
          (Class<? extends T>) Class.forName(value));
      }
    }
  }

  /**
   * Override the default implementation of c, using d instead. d must implement
   * c, of course. If exactly one injectable implementation of c has been
   * registered with Tang (perhaps including c), then this is optional.
   * 
   * @param c
   * @param d
   * @throws NameResolutionException
   */
  public <T> void bindImplementation(Class<T> c, Class<? extends T> d)
      throws NameResolutionException {
    if(sealed) throw new IllegalStateException("Can't bind to sealed Tang!");
    if (!c.isAssignableFrom(d)) {
      throw new ClassCastException(d.getName()
          + " does not extend or implement " + c.getName());
    }
    Node n = namespace.getNode(c);
    if (n instanceof ClassNode) {
      defaultImpls.put((ClassNode<?>)n, d);
    } else {
      // TODO need new exception type here.
      throw new IllegalArgumentException(
          "Detected type mismatch.  Expected ClassNode, but namespace contains a "
              + n);
    }
  }
  private <T> void bindParameter(NamedParameterNode<T> name, String value) {
    if(sealed) throw new IllegalStateException("Can't bind to sealed Tang!");
    T o = ReflectionUtilities.parse(name.argClass, value);
    namedParameters.put(name, value);
    namedParameterInstances.put(name, o);
  }
  /**
   * Set the default value of a named parameter.
   * 
   * @param name
   *          The dummy class that serves as the name of this parameter.
   * @param o
   *          The value of the parameter. The type must match the type specified
   *          by name.
   * @throws NameResolutionException
   */
  @SuppressWarnings("unchecked")
  public <T> void bindParameter(Class<? extends Name<T>> name, String s)
      throws NameResolutionException {
    if(sealed) throw new IllegalStateException("Can't bind to sealed Tang!");
    register(name);
    Node np = namespace.getNode(name);
    if (np instanceof NamedParameterNode) {
      bindParameter((NamedParameterNode<T>)np, s);
    } else {
      // TODO add support for setting default *instance* of class.
      // TODO need new exception type here.
      throw new IllegalArgumentException(
          "Detected type mismatch when setting named parameter " + name
              + "  Expected NamedParameterNode, but namespace contains a " + np);
    }
  }

  public <T> void bindSingleton(Class<T> c) throws NameResolutionException,
      ReflectiveOperationException {
    if(sealed) throw new IllegalStateException("Can't bind to sealed Tang!");
    bindSingleton(c, c);
  }

  @SuppressWarnings("unchecked")
  public <T> void bindSingleton(Class<T> c, Class<? extends T> d)
      throws NameResolutionException, ReflectiveOperationException {
    if(sealed) throw new IllegalStateException("Can't bind to sealed Tang!");

    namespace.register(c);
    namespace.register(d);
    try {
      ClassNode<T> cn = (ClassNode<T>) namespace.getNode(c);
      cn.setIsSingleton();
      singletons.add(cn);
      if(c != d) {
        // Note: d is *NOT* necessarily a singleton.
        defaultImpls.put(cn, d);
      }
    } catch (NameResolutionException e) {
      throw new IllegalStateException("Failed to lookup class " + c
          + " which this method just registered!");
    }
  }

  @SuppressWarnings("unchecked")
  private <T> T injectSingleton(Class<T> c) throws NameResolutionException,
      ReflectiveOperationException {
    namespace.register(c);
    ClassNode<T> cn;
    try {
      cn = (ClassNode<T>) namespace.getNode(c);
    } catch (NameResolutionException e) {
      throw new IllegalStateException("Could not find class " + c
          + " which this method just registered!");
    } catch (ClassCastException e) {
      throw new IllegalArgumentException("Cannot call setClassSingleton on "
          + c + ".  Try setNamedParameter() instead.");
    }
    if (singletonInstances.containsKey(cn)) {
      return (T) singletonInstances.get(cn);
    } else if (cn.getIsSingleton()) {
      throw new IllegalStateException("No instance found for Singleton " + cn);
    } else {
      throw new IllegalStateException("Attempt to inject singleton of " + c
          + " which has not been bound as a singleton.");
    }
  }

  /**
   * This interface allows legacy classes to be injected by Tang. To be of any
   * use, implementations of this class must have at least one constructor with
   * an @Inject annotation. From Tang's perspective, an ExternalConstructor
   * class is just a special instance of the class T, except that, after
   * injection an ExternalConstructor, Tang will call newInstance, and store the
   * resulting object. It will then discard the ExternalConstructor.
   * 
   * @author sears
   * 
   * @param <T>
   *          The type this ExternalConstructor will create.
   */
  public interface ExternalConstructor<T> {
    /**
     * This method will only be called once.
     * 
     * @return a new, distinct instance of T.
     */
    T newInstance();
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public <T> void bindConstructor(Class<T> c,
      Class<? extends ExternalConstructor<? extends T>> v) {
    namespace.register(c);
    System.err.println("Warning: ExternalConstructors aren't implemented at the moment");
    try {
      constructors.put(namespace.getNode(c), (Class) v);
    } catch (NameResolutionException e) {
      throw new IllegalStateException("Could not find class " + c
          + " which this method just registered!");
    }
  }

  static final InjectionPlan BUILDING = new InjectionPlan() {
    @Override
    public int getNumAlternatives() {
      throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
      return "BUILDING INJECTION PLAN";
    }
  };

  private InjectionPlan wrapInjectionPlans(String infeasibleName,
      List<? extends InjectionPlan> list) {
    if (list.size() == 0) {
      return new InfeasibleInjectionPlan(infeasibleName);
    } else if (list.size() == 1) {
      return list.get(0);
    } else {
      return new InjectionPlan.AmbiguousInjectionPlan(
          list.toArray(new InjectionPlan[0]));
    }
  }

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
      NamedParameterNode<?> np = (NamedParameterNode<?>) n;
      Object instance = namedParameterInstances.get(n);
      if (instance == null) {
        instance = np.defaultInstance;
      }
      ip = new Instance(np, instance);
    } else if (n instanceof ClassNode) {
      ClassNode<?> cn = (ClassNode<?>) n;
      if (singletonInstances.containsKey(cn)) {
        ip = new Instance(cn, singletonInstances.get(cn));
      } else if (constructors.containsKey(cn)) {
        throw new UnsupportedOperationException("Vivifiers aren't working yet!");
        // ip = new Instance(cn, null);
      } else if (defaultImpls.containsKey(cn)) {
        String implName = defaultImpls.get(cn).getName();
        buildInjectionPlan(implName, memo);
        ip = memo.get(implName);
      } else {
        List<ClassNode<?>> classNodes = new ArrayList<ClassNode<?>>();
        for (ClassNode<?> c : namespace.getKnownImpls(cn)) {
          classNodes.add(c);
        }
        classNodes.add(cn);
        List<InjectionPlan> sub_ips = new ArrayList<InjectionPlan>();
        for (ClassNode<?> thisCN : classNodes) {
          List<InjectionPlan.Constructor> constructors = new ArrayList<InjectionPlan.Constructor>();
          for (ConstructorDef def : thisCN.injectableConstructors) {
            List<InjectionPlan> args = new ArrayList<InjectionPlan>();
            for (ConstructorArg arg : def.args) {
              String argName = arg.getName(); // getFullyQualifiedName(thisCN.clazz);
              buildInjectionPlan(argName, memo);
              args.add(memo.get(argName));
            }
            constructors.add(new InjectionPlan.Constructor(def, args
                .toArray(new InjectionPlan[0])));
          }
          sub_ips.add(wrapInjectionPlans(thisCN.getName(), constructors));
        }
        ip = wrapInjectionPlans(name, sub_ips);
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

  /**
   * Obtain the effective configuration of this Tang instance. This consists of
   * string-string pairs that could be dumped directly to a Properties file, for
   * example. Currently, this method does not return information about default
   * parameter values that were specified by parameter annotations, or about the
   * auto-discovered stuff in TypeHierarchy.  All of that should be automatically
   * imported as these keys are parsed on the other end.
   * 
   * @return a String to String map
   */
  public Map<String, String> getEffectiveConfiguration() {
    Map<String, String> ret = new MonotonicMap<String, String>();
    for (Node opt : defaultImpls.keySet()) {
      ret.put(opt.getFullName(), defaultImpls.get(opt).getName());
    }
    for (Node opt : constructors.keySet()) {
      ret.put(opt.getFullName(), constructors.get(opt).getName());
    }
    for (Node opt : namedParameters.keySet()) {
      ret.put(opt.getFullName(), namedParameters.get(opt));
    }
    for (Node opt : singletons) {
      ret.put(opt.getFullName(), "tang.singleton");
    }
    return ret;
  }

  /**
   * Return an injection plan for the given class / parameter name. This will be
   * more useful once plans can be serialized / deserialized / pretty printed.
   * 
   * @param name
   *          The name of an injectable class or interface, or a NamedParameter.
   * @return
   * @throws NameResolutionException
   */
  public InjectionPlan getInjectionPlan(String name)
      throws NameResolutionException {
    Map<String, InjectionPlan> memo = new HashMap<String, InjectionPlan>();
    buildInjectionPlan(name, memo);
    return memo.get(name);
  }

  /**
   * Returns true if Tang is ready to instantiate the object named by name.
   * 
   * @param name
   * @return
   * @throws NameResolutionException
   */
  public boolean isInjectable(String name) throws NameResolutionException {
    InjectionPlan p = getInjectionPlan(name);
    return p.isInjectable();
  }

  /**
   * Get a new instance of the class clazz.
   * 
   * @param clazz
   * @return
   * @throws NameResolutionException
   * @throws ReflectiveOperationException
   */
  @SuppressWarnings("unchecked")
  public <U> U getInstance(Class<U> clazz) throws NameResolutionException,
      ReflectiveOperationException {
    register(clazz);
    if(!sealed) {
      sealed = true;
      for(ClassNode<?> cn : singletons) {
        Object o = getInstance(cn.getClazz());
        singletonInstances.put(cn, o);
      }
    }
    InjectionPlan plan = getInjectionPlan(clazz.getName());
    return (U) injectFromPlan(plan);
  }

  @SuppressWarnings("unchecked")
  public <T> T getNamedParameter(Class<? extends Name<T>> clazz)
      throws NameResolutionException, ReflectiveOperationException {
    InjectionPlan plan = getInjectionPlan(clazz.getName());
    return (T) injectFromPlan(plan);
  }

  private Object injectFromPlan(InjectionPlan plan)
      throws ReflectiveOperationException {
    if (plan.getNumAlternatives() == 0) {
      throw new IllegalArgumentException("Attempt to inject infeasible plan: "
          + plan.toPrettyString());
    }
    if (plan.getNumAlternatives() > 1) {
      throw new IllegalArgumentException("Attempt to inject ambiguous plan: "
          + plan.toPrettyString());
    }
    if (plan instanceof InjectionPlan.Instance) {
      return ((InjectionPlan.Instance) plan).instance;
    } else if (plan instanceof InjectionPlan.Constructor) {
      InjectionPlan.Constructor constructor = (InjectionPlan.Constructor) plan;
      Object[] args = new Object[constructor.args.length];
      for (int i = 0; i < constructor.args.length; i++) {
        args[i] = injectFromPlan(constructor.args[i]);
      }
      return constructor.constructor.constructor.newInstance(args);
    } else if (plan instanceof AmbiguousInjectionPlan) {
      AmbiguousInjectionPlan ambiguous = (AmbiguousInjectionPlan) plan;
      for (InjectionPlan p : ambiguous.alternatives) {
        if (p.isInjectable()) {
          return injectFromPlan(p);
        }
      }
      throw new IllegalStateException(
          "Thought there was an injectable plan, but can't find it!");
    } else if (plan instanceof InfeasibleInjectionPlan) {
      throw new IllegalArgumentException("Attempt to inject infeasible plan!");
    } else {
      throw new IllegalStateException("Unknown plan type: " + plan);
    }
  }

  private class MonotonicSet<T> extends HashSet<T> {
    private static final long serialVersionUID = 1L;

    @Override
    public boolean add(T e) {
      if (super.contains(e)) {
        throw new IllegalArgumentException("Attempt to re-add " + e
            + " to MonotonicSet!");
      }
      return super.add(e);
    }

    @Override
    public void clear() {
      throw new UnsupportedOperationException("Attempt to clear MonotonicSet!");
    }

    @Override
    public boolean remove(Object o) {
      throw new UnsupportedOperationException("Attempt to remove " + o
          + " from MonotonicSet!");
    }

    @Override
    public boolean removeAll(Collection<?> c) {
      throw new UnsupportedOperationException(
          "removeAll() doesn't make sense for MonotonicSet!");
    }

    @Override
    public boolean retainAll(Collection<?> c) {
      throw new UnsupportedOperationException(
          "retainAll() doesn't make sense for MonotonicSet!");
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
      throw new UnsupportedOperationException(
          "addAll() not implemennted for MonotonicSet.");
    }
  }

  private class MonotonicMap<T, U> extends HashMap<T, U> {
    private static final long serialVersionUID = 1L;

    @Override
    public U put(T key, U value) {
      U old = super.get(key);
      if (old != null) {
        throw new IllegalArgumentException("Attempt to re-bind: (" + key
            + ") old value: " + old + " new value " + value);
      }
      return super.put(key, value);
    }

    @Override
    public void putAll(Map<? extends T, ? extends U> m) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
      throw new UnsupportedOperationException();
    }

    @Override
    public U remove(Object o) {
      throw new UnsupportedOperationException();
    }
  }

}
