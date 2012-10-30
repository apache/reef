package com.microsoft.tang.implementation;

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.microsoft.tang.Configuration;
import com.microsoft.tang.implementation.TypeHierarchy.Node;

public class ConfigurationImpl implements Configuration {
  final ConfigurationBuilderImpl tang;
  public final static String REGISTERED = "registered";
  public final static String SINGLETON = "singleton";

  ConfigurationImpl(ConfigurationBuilderImpl tang) {
    if(tang.dirtyBit) { throw new IllegalStateException("Can't build ConfigurationImpl from dirty ConfigurationBuilderImpl object!"); }
    this.tang = new ConfigurationBuilderImpl(tang);
  }

  public InjectorImpl injector() {
    return new InjectorImpl(this);
  }

  /* (non-Javadoc)
   * @see com.microsoft.tang.implementation.Configuration#writeConfigurationFile(java.io.PrintStream)
   */
  @Override
  public void writeConfigurationFile(OutputStream o) {
    PrintStream s = new PrintStream(o);
    if (tang.dirtyBit) {
      throw new IllegalStateException(
          "Someone called setVolatileInstance() on this ConfigurationBuilderImpl object.  Refusing to serialize it!");
    }
    Map<String, String> effectiveConfiguration = getConfiguration();
    for (String k : effectiveConfiguration.keySet()) {
      // XXX escaping of strings!!!
      s.println(k + "=" + effectiveConfiguration.get(k));
    }
  }

  /**
   * Obtain the effective configuration of this ConfigurationBuilderImpl instance. This consists
   * of string-string pairs that could be dumped directly to a Properties
   * file, for example. Currently, this method does not return information
   * about default parameter values that were specified by parameter
   * annotations, or about the auto-discovered stuff in TypeHierarchy. All of
   * that should be automatically imported as these keys are parsed on the
   * other end.
   * 
   * @return a String to String map
   */
  public Map<String, String> getConfiguration() {
    if (tang.dirtyBit) {
      throw new IllegalStateException(
          "Someone called setVolatileInstance() on this ConfigurationBuilderImpl object; no introspection allowed!");
    }

    Map<String, String> ret = new HashMap<String, String>();
    for (Class<?> opt : tang.namespace.registeredClasses) {
      ret.put(opt.getName(), REGISTERED);
    }
    for (Node opt : tang.boundImpls.keySet()) {
      ret.put(opt.getFullName(), tang.boundImpls.get(opt).getName());
    }
    for (Node opt : tang.boundConstructors.keySet()) {
      ret.put(opt.getFullName(), tang.boundConstructors.get(opt).getName());
    }
    for (Node opt : tang.namedParameters.keySet()) {
      ret.put(opt.getFullName(), tang.namedParameters.get(opt));
    }
    for (Node opt : tang.singletons) {
      ret.put(opt.getFullName(), SINGLETON);
    }
    return ret;
  }
  static public Configuration processConfiguration(Map<String, String> conf) throws ReflectiveOperationException {
    ConfigurationBuilderImpl t = new ConfigurationBuilderImpl();
    for(Entry<String,String> e : conf.entrySet()) {
      if(SINGLETON.equals(e.getValue())) {
        t.bindSingleton(Class.forName(e.getKey()));
      } else if(REGISTERED.equals(e.getValue())) {
        t.register(Class.forName(e.getKey()));
      } else {
        t.bind(e.getKey(), e.getValue());
      }
    }
    return t.build();
  }
}