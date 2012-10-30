package com.microsoft.tang;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

import com.microsoft.tang.TypeHierarchy.Node;

public class TangConf {
  public Tang tang;

  TangConf(Tang tang) {
    if(tang.dirtyBit) { throw new IllegalStateException("Can't build TangConf from dirty Tang object!"); }
    this.tang = tang.deepCopy();
  }

  public TangInjector injector() {
    return new TangInjector(this);
  }

  public void writeConfigurationFile(PrintStream s) {
    if (tang.dirtyBit) {
      throw new IllegalStateException(
          "Someone called setVolatileInstance() on this Tang object.  Refusing to serialize it!");
    }
    Map<String, String> effectiveConfiguration = getEffectiveConfiguration();
    for (String k : effectiveConfiguration.keySet()) {
      // XXX escaping of strings!!!
      s.println(k + "=" + effectiveConfiguration.get(k));
    }
  }

  /**
   * Obtain the effective configuration of this Tang instance. This consists
   * of string-string pairs that could be dumped directly to a Properties
   * file, for example. Currently, this method does not return information
   * about default parameter values that were specified by parameter
   * annotations, or about the auto-discovered stuff in TypeHierarchy. All of
   * that should be automatically imported as these keys are parsed on the
   * other end.
   * 
   * @return a String to String map
   */
  public Map<String, String> getEffectiveConfiguration() {
    if (tang.dirtyBit) {
      throw new IllegalStateException(
          "Someone called setVolatileInstance() on this Tang object; no introspection allowed!");
    }

    Map<String, String> ret = new HashMap<String, String>();
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
      ret.put(opt.getFullName(), "tang.singleton");
    }
    return ret;
  }
}