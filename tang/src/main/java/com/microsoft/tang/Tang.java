package com.microsoft.tang;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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

import com.microsoft.tang.TypeHierarchy.ClassNode;
import com.microsoft.tang.TypeHierarchy.NamedParameterNode;
import com.microsoft.tang.TypeHierarchy.Node;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.exceptions.NameResolutionException;

public class Tang {
  final TypeHierarchy namespace;
  final Map<ClassNode<?>, Class<?>> defaultImpls = new MonotonicMap<ClassNode<?>, Class<?>>();
  final Map<ClassNode<?>, Class<ExternalConstructor<?>>> constructors = new MonotonicMap<ClassNode<?>, Class<ExternalConstructor<?>>>();
  final Set<ClassNode<?>> singletons = new MonotonicSet<ClassNode<?>>();
  final Map<NamedParameterNode<?>, String> namedParameters = new MonotonicMap<NamedParameterNode<?>, String>();

  // *Not* serialized.
  final Map<ClassNode<?>, Object> singletonInstances = new MonotonicMap<ClassNode<?>, Object>();
  final Map<NamedParameterNode<?>, Object> namedParameterInstances = new MonotonicMap<NamedParameterNode<?>, Object>();
  boolean sealed = false;
  boolean dirtyBit = false;

  public Tang() {
    this(new TangConf[0]);
  }

  Tang deepCopy() {
    return new Tang(this);
  }

  private Tang(Tang t) {
    if(t.dirtyBit) { throw new IllegalArgumentException("Cannot copy a dirty Tang"); }
    try {
      namespace = t.namespace.deepCopy();
      for (ClassNode<?> cn : t.defaultImpls.keySet()) {
        defaultImpls.put((ClassNode<?>) namespace.getNode(cn.getClazz()),
            t.defaultImpls.get(cn));
      }
      for (ClassNode<?> cn : t.constructors.keySet()) {
        constructors.put((ClassNode<?>) namespace.getNode(cn.getClazz()),
            t.constructors.get(cn));
      }
      for (ClassNode<?> c : t.singletons) {
        singletons.add((ClassNode<?>) namespace.getNode(c.getClazz()));
      }
      for (NamedParameterNode<?> np : t.namedParameters.keySet()) {
        namedParameters.put(
            (NamedParameterNode<?>) namespace.getNode(np.getNameClass()),
            t.namedParameters.get(np));
      }
      for (ClassNode<?> cn : t.singletonInstances.keySet()) {
        singletonInstances.put((ClassNode<?>) namespace.getNode(cn.getClazz()),
            t.singletonInstances.get(cn));
      }
      for (NamedParameterNode<?> np : t.namedParameterInstances.keySet()) {
        namedParameterInstances.put((NamedParameterNode<?>)namespace.getNode(np.getNameClass()),
            t.namedParameterInstances.get(np));
      }
    } catch(NameResolutionException e) {
      throw new IllegalStateException("Found a consistency error when copying a Tang: ", e);
    }
  }

  public Tang(TangConf... tangs) {
    namespace = new TypeHierarchy();
    for (TangConf tc : tangs) {
      addConf(tc);
    }
  }

  public void addConf(TangConf tc) {

    throw new UnsupportedOperationException("XXX"); // XXX
  }

  public void register(Class<?> c) {
    namespace.register(c);
  }

  public static TangInjector newInjector(TangConf... args) {
    return args[0].tang.forkConf().injector();
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
  public <T> void bind(String key, String value)
      throws NameResolutionException, ClassNotFoundException {
    if (sealed)
      throw new IllegalStateException("Can't bind to sealed Tang!");
    Node n = namespace.getNode(key);
    /*
     * String longVal = shortNames.get(value); if (longVal != null) value =
     * longVal;
     */
    if (n instanceof NamedParameterNode) {
      bindParameter((NamedParameterNode<?>) n, value);
    } else if (n instanceof ClassNode) {
      Class<T> c = ((ClassNode<T>) n).getClazz();
      Class<?> val = (Class<?>) Class.forName(value);
      if (ExternalConstructor.class.isAssignableFrom(val)
          && (!ExternalConstructor.class.isAssignableFrom(c))) {
        bindConstructor(c,
            (Class<? extends ExternalConstructor<? extends T>>) val);
      } else {
        bindImplementation(c, (Class<? extends T>) Class.forName(value));
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
    if (sealed)
      throw new IllegalStateException("Can't bind to sealed Tang!");
    if (!c.isAssignableFrom(d)) {
      throw new ClassCastException(d.getName()
          + " does not extend or implement " + c.getName());
    }
    Node n = namespace.getNode(c);
    if (n instanceof ClassNode) {
      defaultImpls.put((ClassNode<?>) n, d);
    } else {
      // TODO need new exception type here.
      throw new IllegalArgumentException(
          "Detected type mismatch.  Expected ClassNode, but namespace contains a "
              + n);
    }
  }

  private <T> void bindParameter(NamedParameterNode<T> name, String value) {
    if (sealed)
      throw new IllegalStateException("Can't bind to sealed Tang!");
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
    if (sealed)
      throw new IllegalStateException("Can't bind to sealed Tang!");
    register(name);
    Node np = namespace.getNode(name);
    if (np instanceof NamedParameterNode) {
      bindParameter((NamedParameterNode<T>) np, s);
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
    if (sealed)
      throw new IllegalStateException("Can't bind to sealed Tang!");
    bindSingleton(c, c);
  }

  @SuppressWarnings("unchecked")
  public <T> void bindSingleton(Class<T> c, Class<? extends T> d)
      throws NameResolutionException, ReflectiveOperationException {
    if (sealed)
      throw new IllegalStateException("Can't bind to sealed Tang!");

    namespace.register(c);
    namespace.register(d);
    try {
      Node n = namespace.getNode(c);
      if (!(n instanceof ClassNode)) {
        throw new IllegalArgumentException("Can't bind singleton to " + n
            + " try bindParameter() instead.");
      }
      ClassNode<T> cn = (ClassNode<T>) namespace.getNode(c);
      cn.setIsSingleton();
      singletons.add(cn);
      if (c != d) {
        // Note: d is *NOT* necessarily a singleton.
        defaultImpls.put(cn, d);
      }
    } catch (NameResolutionException e) {
      throw new IllegalStateException("Failed to lookup class " + c
          + " which this method just registered!");
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
    System.err
        .println("Warning: ExternalConstructors aren't implemented at the moment");
    try {
      constructors.put((ClassNode<?>) namespace.getNode(c), (Class) v);
    } catch (NameResolutionException e) {
      throw new IllegalStateException("Could not find class " + c
          + " which this method just registered!");
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          "Cannot register external class constructor for " + c
              + " (which is probably a named parameter)");
    }
  }

  static public TangConf tangConfFromConfigurationFile(File configFileName)
      throws ConfigurationException, NameResolutionException,
      ReflectiveOperationException {
    return tangFromConfigurationFile(configFileName).forkConf();
  }

  public TangConf forkConf() {
    return forkConfImpl(); // XXX new Tang(this).forkConfImpl();
  }

  static private <T> Tang tangFromConfigurationFile(File configFileName)
      throws ConfigurationException, NameResolutionException,
      ReflectiveOperationException {
    Tang t = new Tang();

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
        if (value.equals("tang.singleton")) {
          isSingleton = true;
        }
        if (key.equals("import")) {
          if (isSingleton) {
            throw new IllegalArgumentException(
                "Can't import=tang.singleton.  Makes no sense");
          }
          try {
            t.namespace.register(Class.forName(value));
            String[] tok = value.split(TypeHierarchy.regexp);
            try {
              t.namespace.getNode(tok[tok.length - 1]);
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
          if (isSingleton) {
            t.bindSingleton(Class.forName(key));
          } else {
            t.bind(key, value);
          }
        }
      }
    }
    return t;
  }

  private TangConf forkConfImpl() {
    return new TangConf(this);
  }

  @SuppressWarnings("unchecked")
  public <T> T getNamedParameter(Class<? extends Name<T>> clazz)
      throws NameResolutionException, ReflectiveOperationException {
    InjectionPlan plan = forkConf().injector()
        .getInjectionPlan(clazz.getName());
    // XXX this next line is funny bad.
    return (T) forkConf().injector().injectFromPlan(plan);
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
