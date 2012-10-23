package com.microsoft.tang;

import java.util.ArrayList;
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
  private final Map<Node, Class<?>> defaultImpls = new HashMap<Node, Class<?>>();
  private final Map<Node, Object> defaultInstances = new HashMap<Node, Object>();

  public Tang(TypeHierarchy namespace) {
    this.namespace = namespace;
    namespace.resolveAllClasses();
  }

  public <T> void registerConfigFile(String configFileName)
      throws ConfigurationException, NameResolutionException, ClassNotFoundException {
    Configuration conf = new PropertiesConfiguration(configFileName);
    Iterator<String> it = conf.getKeys();

    Map<String, String> shortNames = new HashMap<String,String>();
    
    while (it.hasNext()) {
      String key = it.next();
      String longName = shortNames.get(key);
      String[] values = conf.getStringArray(key);
      if(longName != null) {
//        System.err.println("Mapped " + key + " to " + longName);
        key = longName;
      }
      for (String value : values) {
        if (key.equals("import")) {
          try {
            namespace.register(Class.forName(value));
            String[] tok = value.split(TypeHierarchy.regexp);
            try {
              namespace.getNode(tok[tok.length-1]);
              throw new IllegalArgumentException("Conflict on short name: " + tok[tok.length-1]);
            } catch(NameResolutionException e) {
              String oldValue = shortNames.put(tok[tok.length-1], value);
              if(oldValue != null) {
                throw new IllegalArgumentException("Name conflict.  " + tok[tok.length-1] + " maps to " + oldValue + " and " + value);
              }
//              System.err.println("Added mapping from " + tok[tok.length-1] + " to " + value);
            }
          } catch (ClassNotFoundException e) {
            // print error message + exit.
          }
        } else {
          Node n = namespace.getNode(key);
          String longVal = shortNames.get(value);
          if(longVal != null) value = longVal;
          if (n instanceof NamedParameterNode) {
            @SuppressWarnings("unchecked")
            NamedParameterNode<T> np = (NamedParameterNode<T>) n;

            setParameterValue(np.clazz, ReflectionUtilities.parse(np.argClass, value));
          } else if(n instanceof ClassNode) {
            setClassImplementation(((ClassNode)n).getClazz(), Class.forName(value));
          }
        }
        // Resolve all classes before processing the next line.  It could be
        // that this line indirectly brings a namespace in or something.
        // Plus, this lets us throw an exception earlier in the file, which
        // helps with debugging.
        
        namespace.resolveAllClasses();
      }
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
    for(Option o : applicationOptions.keySet()) {
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
  public <T> void processCommandLine(String[] args) throws NumberFormatException,
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
      
      NamedParameterNode<T> n = namespace.getNodeFromShortName(shortName);
      if (n != null && value != null) {
        // XXX completely untested.
        if(applicationOptions.containsKey(option)) {
          applicationOptions.get(option).process(option);
        } else {
          setParameterValue((n.clazz),
              ReflectionUtilities.parse(n.argClass, value));
        }
      }
    }
  }

  public Tang() {
    namespace = new TypeHierarchy();
  }

  /**
   * Obtain the effective configuration of this Tang instance.  This consists of
   * string-string pairs that could be dumped directly to a Properties file, for
   * example.  Currently, this method does not return information about default
   * parameter values that were specified by parameter annotations.
   * 
   * @return a String to String map 
   */
  public Map<String, String> getEffectiveConfig() {
    Map<String, String> ret = new HashMap<String,String>();
    for(Node opt : defaultImpls.keySet()) {
      ret.put(opt.getFullName(), defaultImpls.get(opt).toString());
    }
    for(Node opt : defaultInstances.keySet()) {
      ret.put(opt.getFullName(), defaultInstances.get(opt).toString());
    }
    return ret;
  }
  /**
   * Override the default implementation of c, using d instead.  d must implement c,
   * of course.  If exactly one injectable implementation of c has been registered
   * with Tang (perhaps including c), then this is optional.
   * @param c
   * @param d
   * @throws NameResolutionException
   */
  public void setClassImplementation(Class<?> c, Class<?> d)
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
  /**
   * Set the default value of a named parameter.
   * @param name The dummy class that serves as the name of this parameter.
   * @param o The value of the parameter.  The type must match the type specified by name.
   * @throws NameResolutionException
   */
  @SuppressWarnings("unchecked")
  public <T> void setParameterValue(Class<? extends Name<T>> name, T o)
      throws NameResolutionException {
    Node n = namespace.getNode(name);
    if (n instanceof NamedParameterNode) {
      setNamedParameter((NamedParameterNode<T>) n, o);
    } else {
      // TODO add support for setting default *instance* of class.
      // TODO need new exception type here.
      throw new IllegalArgumentException(
          "Detected type mismatch when setting named parameter " + name
              + "  Expected NamedParameterNode, but namespace contains a " + n);
    }
  }

  private <T> void setNamedParameter(NamedParameterNode<T> np, T o) {
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

    @Override
    public String toString() {
      return "BUILDING INJECTION PLAN";
    }
  };
  private InjectionPlan wrapInjectionPlans(String infeasibleName, List<? extends InjectionPlan> list) {
    if(list.size() == 0) {
      return new InfeasibleInjectionPlan(infeasibleName);
    } else if(list.size() == 1) {
      return list.get(0);
    } else {
      return new InjectionPlan.AmbiguousInjectionPlan(list.toArray(new InjectionPlan[0]));
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
      Object instance = defaultInstances.get(n);
      if(instance == null) { instance = np.defaultInstance; }
      ip = new Instance(np, instance);
    } else if (n instanceof ClassNode) {
      ClassNode cn = (ClassNode) n;
      if (defaultInstances.containsKey(cn)) {
        ip = new Instance(cn, defaultInstances.get(cn));
      } else if (defaultImpls.containsKey(cn)) {
        String implName = defaultImpls.get(cn).getName();
        buildInjectionPlan(implName, memo);
        ip = memo.get(implName);
      } else {
        List<ClassNode> classNodes = new ArrayList<ClassNode>();
        for (ClassNode c : namespace.getKnownImpls(cn)) {
          classNodes.add(c);
        }
        classNodes.add(cn);
        List<InjectionPlan> sub_ips = new ArrayList<InjectionPlan>();
        for (ClassNode thisCN : classNodes) {
          List<InjectionPlan.Constructor> constructors = new ArrayList<InjectionPlan.Constructor>();
          for (ConstructorDef def : thisCN.injectableConstructors) {
            List<InjectionPlan> args = new ArrayList<InjectionPlan>();
            for (ConstructorArg arg : def.args) {
              String argName = arg.getName(); //getFullyQualifiedName(thisCN.clazz);
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
   * Return an injection plan for the given class / parameter name.  This will be more
   * useful once plans can be serialized / deserialized / pretty printed.
   * 
   * @param name The name of an injectable class or interface, or a NamedParameter.
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
   * @param name
   * @return
   * @throws NameResolutionException
   */
  public boolean canInject(String name) throws NameResolutionException {
    InjectionPlan p = getInjectionPlan(name);
    boolean ret = p.getNumAlternatives() == 1;
    return ret;
  }
  /**
   * Get a new instance of the class clazz.
   * @param clazz
   * @return
   * @throws NameResolutionException
   * @throws ReflectiveOperationException
   */
  @SuppressWarnings("unchecked")
  public <U> U getInstance(Class<U> clazz) throws NameResolutionException,
      ReflectiveOperationException {
    namespace.resolveAllClasses();
    InjectionPlan plan = getInjectionPlan(clazz.getName());
    return (U)injectFromPlan(plan);
  }
  private Object injectFromPlan(InjectionPlan plan) throws ReflectiveOperationException {
    if(plan.getNumAlternatives() == 0) {
      throw new IllegalArgumentException("Attempt to inject infeasible plan: " + InjectionPlan.prettyPrint(plan));
    }
    if(plan.getNumAlternatives() > 1) {
      throw new IllegalArgumentException("Attempt to inject ambiguous plan: " + InjectionPlan.prettyPrint(plan));
    }
    if(plan instanceof InjectionPlan.Instance) {
      return ((InjectionPlan.Instance)plan).instance;
    } else if(plan instanceof InjectionPlan.Constructor) {
      InjectionPlan.Constructor constructor = (InjectionPlan.Constructor)plan;
      Object[] args = new Object[constructor.args.length];
      for(int i = 0; i < constructor.args.length; i++) {
        args[i] = injectFromPlan(constructor.args[i]);
      }
      return constructor.constructor.constructor.newInstance(args);
    } else if(plan instanceof AmbiguousInjectionPlan) {
      AmbiguousInjectionPlan ambiguous = (AmbiguousInjectionPlan)plan;
      for(InjectionPlan p : ambiguous.alternatives) {
        if(p.canInject()) { return injectFromPlan(p); }
      }
      throw new IllegalStateException("Thought there was an injectable plan, but can't find it!");
    } else if(plan instanceof InfeasibleInjectionPlan) {
      throw new IllegalArgumentException("Attempt to inject infeasible plan!");
    } else {
      throw new IllegalStateException("Unknown plan type: " + plan);
    }
  }
}
