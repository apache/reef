package com.microsoft.tang.implementation;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import com.microsoft.tang.Configuration;
import com.microsoft.tang.ConfigurationBuilder;
import com.microsoft.tang.ExternalConstructor;
import com.microsoft.tang.Injector;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.NameResolutionException;
import com.microsoft.tang.implementation.TypeHierarchy.ClassNode;
import com.microsoft.tang.implementation.TypeHierarchy.NamedParameterNode;
import com.microsoft.tang.implementation.TypeHierarchy.Node;
import com.microsoft.tang.util.ReflectionUtilities;

public class ConfigurationBuilderImpl implements ConfigurationBuilder {
  private final ConfigurationImpl conf;

  ConfigurationBuilderImpl(ConfigurationBuilderImpl t) {
    conf = new ConfigurationImpl(t.conf.loaders);
    try {
      addConfiguration(t);
    } catch (BindException e) {
      throw new IllegalStateException("Could not copy builder", e);
    }
  }

  ConfigurationBuilderImpl() {
    conf = new ConfigurationImpl();
  }

  ConfigurationBuilderImpl(ClassLoader... loaders) {
    conf = new ConfigurationImpl(loaders);
  }

  ConfigurationBuilderImpl(Configuration... tangs) throws BindException {
    conf = new ConfigurationImpl();
    for (Configuration tc : tangs) {
      addConfiguration(((ConfigurationImpl) tc));
    }
  }

  private void addConfiguration(ConfigurationBuilderImpl tc)
      throws BindException {
    addConfiguration(tc.conf);
  }

  @Override
  public void addConfiguration(Configuration ti) throws BindException {
    ConfigurationImpl t = (ConfigurationImpl) ti;
    if (t.dirtyBit) {
      throw new IllegalArgumentException(
          "Cannot copy a dirty ConfigurationBuilderImpl");
    }
    try {
      for (Class<?> c : t.namespace.registeredClasses) {
        register(c);
      }
      // Note: The commented out lines would be faster, but, for testing
      // purposes,
      // we run through the high-level bind(), which dispatches to the correct
      // call.
      for (ClassNode<?> cn : t.boundImpls.keySet()) {
        bind(cn.getClazz(), t.boundImpls.get(cn));
        // bindImplementation((Class<?>) cn.getClazz(), (Class)
        // t.boundImpls.get(cn));
      }
      for (ClassNode<?> cn : t.boundConstructors.keySet()) {
        bind(cn.getClazz(), t.boundConstructors.get(cn));
        // bindConstructor((Class<?>) cn.getClazz(), (Class)
        // t.boundConstructors.get(cn));
      }
      for (ClassNode<?> cn : t.singletons) {
        try {
          bindSingleton(cn.getClazz());
        } catch (BindException e) {
          throw new IllegalStateException(
              "Unexpected BindException when copying ConfigurationBuilderImpl",
              e);
        }
      }
      for (NamedParameterNode<?> np : t.namedParameters.keySet()) {
        bind(np.getNameClass().getName(), t.namedParameters.get(np));
        // bindParameter(np.getNameClass(), t.namedParameters.get(np));
      }
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(
          "Encountered reflection error when copying a ConfigurationBuilderImpl: ",
          e);
    }
  }

  /**
   * Needed when you want to make a class available for injection, but don't
   * want to bind a subclass to its implementation. Without this call, by the
   * time injector.newInstance() is called, ConfigurationBuilderImpl has been
   * locked down, and the class won't be found.
   * 
   * @param c
   */
  @Override
  public void register(Class<?> c) throws BindException {
    conf.namespace.register(c);
  }

  public static Injector newInjector(ConfigurationImpl... args) {
    return args[0].injector();
  }

  private Options getCommandLineOptions() {
    Options opts = new Options();
    Collection<NamedParameterNode<?>> namedParameters = conf.namespace
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

  @Override
  public void addCommandLineOption(Option option, CommandLineCallback cb) {
    // TODO: Check for conflicting options.
    applicationOptions.put(option, cb);
  }

  @Override
  public <T> void processCommandLine(String[] args) throws IOException,
      BindException {
    Options o = getCommandLineOptions();
    Option helpFlag = new Option("?", "help");
    o.addOption(helpFlag);
    Parser g = new GnuParser();
    CommandLine cl;
    try {
      cl = g.parse(o, args);
    } catch (ParseException e) {
      throw new IOException("Could not parse config file", e);
    }
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

      NamedParameterNode<T> n = conf.namespace.getNodeFromShortName(shortName);
      if (n != null && value != null) {
        // XXX completely untested.

        if (applicationOptions.containsKey(option)) {
          applicationOptions.get(option).process(option);
        } else {
          bindNamedParameter(n.clazz, value);
        }
      }
    }
  }

  @Override
  public <T> void bind(String key, String value) throws ClassNotFoundException,
      BindException {
    if (conf.sealed)
      throw new IllegalStateException(
          "Can't bind to sealed ConfigurationBuilderImpl!");
    Node n = conf.namespace.register(conf.classForName(key));
    /*
     * String longVal = shortNames.get(value); if (longVal != null) value =
     * longVal;
     */
    if (n instanceof NamedParameterNode) {
      bindParameter((NamedParameterNode<?>) n, value);
    } else if (n instanceof ClassNode) {
      bind(((ClassNode<?>) n).getClazz(), conf.classForName(value));
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> void bind(Class<T> c, Class<?> val) throws BindException {
    if (ExternalConstructor.class.isAssignableFrom(val)
        && (!ExternalConstructor.class.isAssignableFrom(c))) {
      bindConstructor(c,
          (Class<? extends ExternalConstructor<? extends T>>) val);
    } else {
      bindImplementation(c, (Class<? extends T>) val);
    }
  }

  @Override
  public <T> void bindImplementation(Class<T> c, Class<? extends T> d)
      throws BindException {
    if (conf.sealed)
      throw new IllegalStateException(
          "Can't bind to sealed ConfigurationBuilderImpl!");
    if (!c.isAssignableFrom(d)) {
      throw new ClassCastException(d.getName()
          + " does not extend or implement " + c.getName());
    }

    Node n = conf.namespace.register(c);
    conf.namespace.register(d);

    if (n instanceof ClassNode) {
      conf.boundImpls.put((ClassNode<?>) n, d);
    } else {
      throw new BindException(
          "Detected type mismatch.  bindImplementation needs a ClassNode, but "
              + "namespace contains a " + n);
    }
  }

  private <T> void bindParameter(NamedParameterNode<T> name, String value) {
    if (conf.sealed)
      throw new IllegalStateException(
          "Can't bind to sealed ConfigurationBuilderImpl!");
    T o = ReflectionUtilities.parse(name.argClass, value);
    conf.namedParameters.put(name, value);
    conf.namedParameterInstances.put(name, o);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> void bindNamedParameter(Class<? extends Name<T>> name, String s)
      throws BindException {
    if (conf.sealed)
      throw new IllegalStateException(
          "Can't bind to sealed ConfigurationBuilderImpl!");
    Node np = conf.namespace.register(name);
    if (np instanceof NamedParameterNode) {
      bindParameter((NamedParameterNode<T>) np, s);
    } else {
      throw new BindException(
          "Detected type mismatch when setting named parameter " + name
              + "  Expected NamedParameterNode, but namespace contains a " + np);
    }
  }

  @Override
  public <T> void bindSingleton(Class<T> c) throws BindException {
    if (conf.sealed)
      throw new IllegalStateException(
          "Can't bind to sealed ConfigurationBuilderImpl!");
    bindSingletonImplementation(c, c);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> void bindSingletonImplementation(Class<T> c, Class<? extends T> d)
      throws BindException {
    if (conf.sealed)
      throw new IllegalStateException(
          "Can't bind to sealed ConfigurationBuilderImpl!");

    Node n = conf.namespace.register(c);
    conf.namespace.register(d);

    if (!(n instanceof ClassNode)) {
      throw new IllegalArgumentException("Can't bind singleton to " + n
          + " try bindParameter() instead.");
    }
    ClassNode<T> cn = (ClassNode<T>) n;
    cn.setIsSingleton();
    conf.singletons.add(cn);
    if (c != d) {
      // Note: d is *NOT* necessarily a singleton.
      conf.boundImpls.put(cn, d);
    }
  }

  @Override
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public <T> void bindConstructor(Class<T> c,
      Class<? extends ExternalConstructor<? extends T>> v) throws BindException {
    System.err
        .println("Warning: ExternalConstructors aren't implemented at the moment");
    try {
      conf.boundConstructors.put((ClassNode<?>) conf.namespace.register(c),
          (Class) v);
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          "Cannot register external class constructor for " + c
              + " (which is probably a named parameter)");
    }
  }

  @Override
  public ConfigurationImpl build() {
    ConfigurationBuilderImpl b = new ConfigurationBuilderImpl(this);
    return b.conf;
  }

  @Override
  public void processConfigFile(File file) throws IOException, BindException {
    PropertiesConfiguration confFile;
    try {
      confFile = new PropertiesConfiguration(file);
    } catch (ConfigurationException e) {
      throw new BindException("Problem parsing config file", e);
    }
    Iterator<String> it = confFile.getKeys();
    Map<String, String> shortNames = new HashMap<String, String>();

    while (it.hasNext()) {
      String key = it.next();
      String longName = shortNames.get(key);
      String[] values = confFile.getStringArray(key);
      if (longName != null) {
        // System.err.println("Mapped " + key + " to " + longName);
        key = longName;
      }
      for (String value : values) {
        boolean isSingleton = false;
        if (value.equals(ConfigurationImpl.SINGLETON)) {
          isSingleton = true;
        }
        if (value.equals(ConfigurationImpl.REGISTERED)) {
          try {
            this.conf.namespace.register(conf.classForName(key));
          } catch (ClassNotFoundException e) {
            throw new BindException("Could not find class " + key
                + " from config file", e);
          }
        }
        if (key.equals(ConfigurationImpl.IMPORT)) {
          if (isSingleton) {
            throw new IllegalArgumentException("Can't "
                + ConfigurationImpl.IMPORT + "=" + ConfigurationImpl.SINGLETON
                + ".  Makes no sense");
          }
          try {
            this.conf.namespace.register(conf.classForName(value));
            String[] tok = value.split(TypeHierarchy.regexp);
            try {
              this.conf.namespace.getNode(tok[tok.length - 1]);
              throw new IllegalArgumentException("Conflict on short name: "
                  + tok[tok.length - 1]);
            } catch (NameResolutionException e) {
              String oldValue = shortNames.put(tok[tok.length - 1], value);
              if (oldValue != null) {
                throw new IllegalArgumentException("Name conflict.  "
                    + tok[tok.length - 1] + " maps to " + oldValue + " and "
                    + value);
              }
            }
          } catch (ClassNotFoundException e) {
            throw new BindException("Could not find class " + value
                + " in config file", e);
          }
        } else {
          if (isSingleton) {
            final Class<?> c;
            try {
              c = conf.classForName(key);
            } catch (ClassNotFoundException e) {
              throw new BindException(
                  "Could not find class to be bound as singleton", e);
            }
            bindSingleton(c);
          } else {
            try {
              bind(key, value);
            } catch (ClassNotFoundException e) {
              throw new BindException(
                  "Could not find class mentioned by config file", e);
            }
          }
        }
      }
    }
  }
}
