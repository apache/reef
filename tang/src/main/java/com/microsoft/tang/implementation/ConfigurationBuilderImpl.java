package com.microsoft.tang.implementation;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import com.microsoft.tang.Configuration;
import com.microsoft.tang.ConfigurationBuilder;
import com.microsoft.tang.ExternalConstructor;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.NameResolutionException;
import com.microsoft.tang.implementation.Node.ClassNode;
import com.microsoft.tang.implementation.Node.NamedParameterNode;
import com.microsoft.tang.util.ReflectionUtilities;

public class ConfigurationBuilderImpl implements ConfigurationBuilder {
  private final ConfigurationImpl conf;

  ConfigurationBuilderImpl(ConfigurationBuilderImpl t) {
    conf = new ConfigurationImpl();
    try {
      addConfiguration(t);
    } catch (BindException e) {
      throw new IllegalStateException("Could not copy builder", e);
    }
  }

  ConfigurationBuilderImpl(URL... jars) {
    conf = new ConfigurationImpl(jars);
  }

  ConfigurationBuilderImpl(Configuration tang) {
    try {
      conf = new ConfigurationImpl();
      addConfiguration(tang);
    } catch(BindException e) {
      throw new IllegalStateException("Error copying Configuration.", e);
    }
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
    ConfigurationImpl old = (ConfigurationImpl) ti;
    if (old.dirtyBit) {
      throw new IllegalArgumentException(
          "Cannot copy a dirty ConfigurationBuilderImpl");
    }
    conf.namespace.addJars(old.namespace.getJars());
    
    for (String s : old.namespace.getRegisteredClassNames()) {
      register(s);
    }
    // Note: The commented out lines would be faster, but, for testing
    // purposes,
    // we run through the high-level bind(), which dispatches to the correct
    // call.
    for (ClassNode<?> cn : old.boundImpls.keySet()) {
      bind(cn.getClazz(), old.boundImpls.get(cn));
      // bindImplementation((Class<?>) cn.getClazz(), (Class)
      // t.boundImpls.get(cn));
    }
    for (ClassNode<?> cn : old.boundConstructors.keySet()) {
      bind(cn.getClazz(), old.boundConstructors.get(cn));
      // bindConstructor((Class<?>) cn.getClazz(), (Class)
      // t.boundConstructors.get(cn));
    }
    for (ClassNode<?> cn : old.singletons) {
      try {
        Class<?> clazz = cn.getClazz();
        Object o = old.singletonInstances.get(cn);
        if(o != null) {
          ClassNode<?> new_cn= (ClassNode<?>)conf.namespace.register(ReflectionUtilities.getFullName(clazz));
          conf.singletons.add(new_cn);
          conf.singletonInstances.put(new_cn, o);
        } else {
          bindSingleton(clazz);
        }
      } catch (BindException e) {
        throw new IllegalStateException(
            "Unexpected BindException when copying ConfigurationBuilderImpl",
            e);
      }
    }
    // The namedParameters set contains the strings that can be used to instantiate new
    // named parameter instances.  Create new ones where we can.
    for (NamedParameterNode<?> np : old.namedParameters.keySet()) {
      bind(np.getNameClass().getName(), old.namedParameters.get(np));
    }
    // Copy references to the remaining (which must have been set with bindVolatileParameter())
    for (NamedParameterNode<?> np : old.namedParameterInstances.keySet()) {
      if(!old.namedParameters.containsKey(np)) {
        Object o = old.namedParameterInstances.get(np);
        NamedParameterNode<?> new_np= (NamedParameterNode<?>)conf.namespace.register(ReflectionUtilities.getFullName(np.getNameClass()));
        conf.namedParameterInstances.put(new_np, o);
        if(o instanceof Class) {
          register((Class<?>)o);
        }
      }
    }
    for (ClassNode<?> cn : old.legacyConstructors.keySet()) {
      registerLegacyConstructor(cn.getClazz(), old.legacyConstructors.get(cn).getConstructor().getParameterTypes());
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
    conf.namespace.register(ReflectionUtilities.getFullName(c));
  }
  public void register(String s)  throws BindException {
    conf.namespace.register(s);
  }

  @Override
  public <T> void registerLegacyConstructor(Class<T> c, final Class<?>... args) throws BindException {
    @SuppressWarnings("unchecked")
    ClassNode<T> cn = (ClassNode<T>) conf.namespace.register(ReflectionUtilities.getFullName(c));
    conf.legacyConstructors.put(cn, cn.createConstructorDef(args));
  }
  

  @Override
  public <T> void bind(String key, String value) throws BindException {
    if (conf.sealed)
      throw new IllegalStateException(
          "Can't bind to sealed ConfigurationBuilderImpl!");
    Node n = conf.namespace.register(key);
    if (n instanceof NamedParameterNode) {
      bindParameter((NamedParameterNode<?>) n, value);
    } else if (n instanceof ClassNode) {
      Class<?> v;
      try {
        v = conf.namespace.classForName(value);
      } catch(ClassNotFoundException e) {
        throw new BindException("Could not find class " + value);
      }
      bind(((ClassNode<?>) n).getClazz(), v);
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

    Node n = conf.namespace.register(ReflectionUtilities.getFullName(c));
    conf.namespace.register(ReflectionUtilities.getFullName(d));

    if (n instanceof ClassNode) {
      conf.boundImpls.put((ClassNode<?>) n, d);
    } else {
      throw new BindException(
          "Detected type mismatch.  bindImplementation needs a ClassNode, but "
              + "namespace contains a " + n);
    }
  }
  @SuppressWarnings("unchecked")
  private <T> void bindParameter(NamedParameterNode<T> name, String value) throws BindException {
    if (conf.sealed)
      throw new IllegalStateException(
          "Can't bind to sealed ConfigurationBuilderImpl!");
    T o;
    try {
      o = ReflectionUtilities.parse(name.getArgClass(), value);
    } catch(UnsupportedOperationException e) {
      try {
        o = (T)conf.namespace.classForName(value);
      } catch (ClassNotFoundException e1) {
        throw new BindException("Do not know how to parse a " + name.getArgClass() + " Furthermore, could not bind it to an implementation with name " + value);
      }
    }
    conf.namedParameters.put(name, value);
    conf.namedParameterInstances.put(name, o);
    if(o instanceof Class) {
      register((Class<?>)o);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> void bindNamedParameter(Class<? extends Name<T>> name, String s)
      throws BindException {
    if (conf.sealed)
      throw new IllegalStateException(
          "Can't bind to sealed ConfigurationBuilderImpl!");
    Node np = conf.namespace.register(ReflectionUtilities.getFullName(name));
    if (np instanceof NamedParameterNode) {
      bindParameter((NamedParameterNode<T>) np, s);
    } else {
      throw new BindException(
          "Detected type mismatch when setting named parameter " + name
              + "  Expected NamedParameterNode, but namespace contains a " + np);
    }
  }

  @Override
  public <T> void bindNamedParameter(Class<? extends Name<T>> iface,
      Class<? extends T> impl) throws BindException {
    if (conf.sealed)
        throw new IllegalStateException(
            "Can't bind to sealed ConfigurationBuilderImpl!");
    Node n = conf.namespace.register(ReflectionUtilities.getFullName(iface));
    conf.namespace.register(ReflectionUtilities.getFullName(impl));
    if(n instanceof NamedParameterNode) {
      bindParameter((NamedParameterNode<?>) n, impl.getName());
    } else {
      throw new BindException(
          "Detected type mismatch when setting named parameter " + iface
              + "  Expected NamedParameterNode, but namespace contains a " + n);
    }
    
    
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> void bindSingleton(Class<T> c) throws BindException {
    if (conf.sealed)
      throw new IllegalStateException(
          "Can't bind to sealed ConfigurationBuilderImpl!");

    Node n = conf.namespace.register(ReflectionUtilities.getFullName(c));

    if (!(n instanceof ClassNode)) {
      throw new IllegalArgumentException("Can't bind singleton to " + n
          + " try bindParameter() instead.");
    }
    ClassNode<T> cn = (ClassNode<T>) n;
    conf.singletons.add(cn);
  }

  @Override
  public <T> void bindSingletonImplementation(Class<T> c, Class<? extends T> d)
        throws BindException {
      bindSingleton(c);
      bindImplementation(c, d);
  }

  @Override
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public <T> void bindConstructor(Class<T> c,
      Class<? extends ExternalConstructor<? extends T>> v) throws BindException {

    conf.namespace.register(ReflectionUtilities.getFullName(v));
    try {
      conf.boundConstructors.put((ClassNode<?>) conf.namespace.register(ReflectionUtilities.getFullName(c)),
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
  public void addConfiguration(File file) throws IOException, BindException {
    PropertiesConfiguration confFile;
    try {
      confFile = new PropertiesConfiguration(file);
    } catch (ConfigurationException e) {
      throw new BindException("Problem parsing config file", e);
    }
    processConfigFile(confFile);
  }
  
  @Override
  public final void addConfiguration(String conf) throws BindException {
	  try {
		File tmp = File.createTempFile("tang", "tmp");
		FileOutputStream fos = new FileOutputStream(tmp);
		fos.write(conf.getBytes());
		fos.close();
		addConfiguration(tmp);
		tmp.delete();
	} catch (IOException e) {
		e.printStackTrace();
	}
  }
  
  
  public void processConfigFile(PropertiesConfiguration confFile) throws IOException, BindException {
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
          this.conf.namespace.register(key);
        } else if (key.equals(ConfigurationImpl.IMPORT)) {
          if (isSingleton) {
            throw new IllegalArgumentException("Can't "
                + ConfigurationImpl.IMPORT + "=" + ConfigurationImpl.SINGLETON
                + ".  Makes no sense");
          }
          this.conf.namespace.register(value);
          String[] tok = value.split(ReflectionUtilities.regexp);
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
        } else if(value.startsWith(ConfigurationImpl.INIT)) {
          String parseValue = value.substring(ConfigurationImpl.INIT.length(), value.length());
          parseValue = parseValue.replaceAll("^[\\s\\(]+", "");
          parseValue = parseValue.replaceAll("[\\s\\)]+$", "");
          String[] classes = parseValue.split("[\\s\\-]+");
          Class<?>[] clazzes = new Class[classes.length];
          for(int i = 0; i < classes.length; i++) {
            try {
              clazzes[i] = conf.namespace.classForName(classes[i]);
            } catch (ClassNotFoundException e) {
              throw new BindException("Could not find arg " + classes[i] + " of constructor for " + key);
            }
          }
          try {
            registerLegacyConstructor(conf.namespace.classForName(key), clazzes);
          } catch (ClassNotFoundException e) {
            throw new BindException("Could not find class " + key + " when trying to register legacy constructor " + value);
          }
        } else {
          if (isSingleton) {
            final Class<?> c;
            try {
              c = conf.namespace.classForName(key);
            } catch (ClassNotFoundException e) {
              throw new BindException(
                  "Could not find class to be bound as singleton", e);
            }
            bindSingleton(c);
          } else {
            bind(key, value);
          }
        }
      }
    }
  }

  @Override
  public Collection<String> getShortNames() {
    return conf.namespace.getShortNames();
  }

  @Override
  public String resolveShortName(String shortName) throws BindException {
    String ret = conf.namespace.resolveShortName(shortName);
    if(ret == null) {
      throw new BindException("Could not find requested shortName:" + shortName);
    }
    return ret;
  }

  @Override
  public String classPrettyDefaultString(String longName) throws BindException {
    try {
      NamedParameterNode<?> param = (NamedParameterNode<?>)conf.namespace.getNode(longName);
      return param.getArgClass().getSimpleName() + "=" + param.getDefaultInstance();
    } catch (NameResolutionException e) {
      throw new BindException("Couldn't find " + longName + " when looking for default value", e);
    }
  }

  @Override
  public String classPrettyDescriptionString(String longName) throws BindException {
    try {
      NamedParameterNode<?> param = (NamedParameterNode<?>)conf.namespace.getNode(longName);
      return param.getDocumentation() + "\n" + param.getFullName();
    } catch (NameResolutionException e) {
      throw new BindException("Couldn't find " + longName + " when looking for documentation string", e);
    }
    
  }
}
