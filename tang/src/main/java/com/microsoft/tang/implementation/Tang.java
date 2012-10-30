package com.microsoft.tang.implementation;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
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

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.exceptions.NameResolutionException;
import com.microsoft.tang.implementation.TypeHierarchy.ClassNode;
import com.microsoft.tang.implementation.TypeHierarchy.NamedParameterNode;
import com.microsoft.tang.implementation.TypeHierarchy.Node;
import com.microsoft.tang.util.MonotonicMap;
import com.microsoft.tang.util.MonotonicSet;
import com.microsoft.tang.util.ReflectionUtilities;

public class Tang {
  public final TypeHierarchy namespace = new TypeHierarchy();
  final Map<ClassNode<?>, Class<?>> boundImpls = new MonotonicMap<ClassNode<?>, Class<?>>();
  final Map<ClassNode<?>, Class<ExternalConstructor<?>>> boundConstructors = new MonotonicMap<ClassNode<?>, Class<ExternalConstructor<?>>>();
  final Set<ClassNode<?>> singletons = new MonotonicSet<ClassNode<?>>();
  final Map<NamedParameterNode<?>, String> namedParameters = new MonotonicMap<NamedParameterNode<?>, String>();
  
  // *Not* serialized.
  final Map<ClassNode<?>, Object> singletonInstances = new MonotonicMap<ClassNode<?>, Object>();
  final Map<NamedParameterNode<?>, Object> namedParameterInstances = new MonotonicMap<NamedParameterNode<?>, Object>();
  boolean sealed = false;
  boolean dirtyBit = false;

  public Tang() {
  }

  Tang(Tang t) {
    addConf(t);
  }

  public Tang(TangConf... tangs) {
    for (TangConf tc : tangs) {
      addConf(tc.tang);
    }
  }

//  @SuppressWarnings({"unchecked", "rawtypes"})
  private void addConf(Tang t) {
    if (t.dirtyBit) {
      throw new IllegalArgumentException("Cannot copy a dirty Tang");
    }
    try {
      for (Class<?> c : t.namespace.registeredClasses) {
        register(c);
      }
      // Note: The commented out lines would be faster, but, for testing purposes, 
      // we run through the high-level bind(), which dispatches to the correct call.
      for (ClassNode<?> cn : t.boundImpls.keySet()) {
        bind(cn.getClazz(), t.boundImpls.get(cn));
//        bindImplementation((Class<?>) cn.getClazz(), (Class) t.boundImpls.get(cn));
      }
      for (ClassNode<?> cn : t.boundConstructors.keySet()) {
        bind(cn.getClazz(), t.boundConstructors.get(cn));
//        bindConstructor((Class<?>) cn.getClazz(), (Class) t.boundConstructors.get(cn));
      }
      for (ClassNode<?> cn : t.singletons) {
        bindSingleton(cn.getClazz());
      }
      for (NamedParameterNode<?> np : t.namedParameters.keySet()) {
        bind(np.getNameClass().getName(), t.namedParameters.get(np));
//        bindParameter(np.getNameClass(), t.namedParameters.get(np));
      }
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(
          "Encountered reflection error when copying a Tang: ", e);
    }
  }

  /**
   * Needed when you want to make a class available for injection, but don't
   * want to bind a subclass to its implementation. Without this call, by the
   * time injector.newInstance() is called, Tang has been locked down, and the
   * class won't be found.
   * 
   * @param c
   */
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
      throws NumberFormatException, ParseException {
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

  public <T> void bind(String key, String value) throws ClassNotFoundException {
    if (sealed)
      throw new IllegalStateException("Can't bind to sealed Tang!");
    Node n = namespace.register(Class.forName(key));
    /*
     * String longVal = shortNames.get(value); if (longVal != null) value =
     * longVal;
     */
    if (n instanceof NamedParameterNode) {
      bindParameter((NamedParameterNode<?>) n, value);
    } else if (n instanceof ClassNode) {
      bind(((ClassNode<?>) n).getClazz(), Class.forName(value));
    }
  }
  @SuppressWarnings("unchecked")
  public <T> void bind(Class<T> c, Class<?> val) {
    if (ExternalConstructor.class.isAssignableFrom(val)
        && (!ExternalConstructor.class.isAssignableFrom(c))) {
      bindConstructor(c, (Class<? extends ExternalConstructor<? extends T>>)val);
    } else {
      bindImplementation(c, (Class<? extends T>)val);
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
  public <T> void bindImplementation(Class<T> c, Class<? extends T> d) {
    if (sealed)
      throw new IllegalStateException("Can't bind to sealed Tang!");
    if (!c.isAssignableFrom(d)) {
      throw new ClassCastException(d.getName()
          + " does not extend or implement " + c.getName());
    }
    Node n = namespace.register(c);
    namespace.register(d);

    if (n instanceof ClassNode) {
      boundImpls.put((ClassNode<?>) n, d);
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
  public <T> void bindParameter(Class<? extends Name<T>> name, String s) {
    if (sealed)
      throw new IllegalStateException("Can't bind to sealed Tang!");
    Node np = namespace.register(name);
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

  public <T> void bindSingleton(Class<T> c) throws ReflectiveOperationException {
    if (sealed)
      throw new IllegalStateException("Can't bind to sealed Tang!");
    bindSingleton(c, c);
  }

  @SuppressWarnings("unchecked")
  public <T> void bindSingleton(Class<T> c, Class<? extends T> d)
      throws ReflectiveOperationException {
    if (sealed)
      throw new IllegalStateException("Can't bind to sealed Tang!");

    Node n = namespace.register(c);
    namespace.register(d);

    if (!(n instanceof ClassNode)) {
      throw new IllegalArgumentException("Can't bind singleton to " + n
          + " try bindParameter() instead.");
    }
    ClassNode<T> cn = (ClassNode<T>) n;
    cn.setIsSingleton();
    singletons.add(cn);
    if (c != d) {
      // Note: d is *NOT* necessarily a singleton.
      boundImpls.put(cn, d);
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
    System.err
        .println("Warning: ExternalConstructors aren't implemented at the moment");
    try {
      boundConstructors.put((ClassNode<?>) namespace.register(c), (Class) v);
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          "Cannot register external class constructor for " + c
              + " (which is probably a named parameter)");
    }
  }

  static public TangConf tangConfFromConfigurationFile(File configFileName)
      throws ConfigurationException, ReflectiveOperationException {
    return tangFromConfigurationFile(configFileName).forkConf();
  }

  public TangConf forkConf() {
    return forkConfImpl(); // XXX new Tang(this).forkConfImpl();
  }

  static private <T> Tang tangFromConfigurationFile(File configFileName)
      throws ConfigurationException, ReflectiveOperationException {
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

}
