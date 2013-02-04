package com.microsoft.tang.implementation.java;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.microsoft.tang.ClassNode;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.ConstructorArg;
import com.microsoft.tang.ConstructorDef;
import com.microsoft.tang.ExternalConstructor;
import com.microsoft.tang.Injector;
import com.microsoft.tang.NamedParameterNode;
import com.microsoft.tang.NamespaceNode;
import com.microsoft.tang.Node;
import com.microsoft.tang.PackageNode;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.exceptions.NameResolutionException;
import com.microsoft.tang.util.ReflectionUtilities;

public class InjectorImpl implements Injector {
  private class SingletonInjectionException extends InjectionException {
    private static final long serialVersionUID = 1L;

    SingletonInjectionException(String s) {
      super(s);
    }
  }

  private final ConfigurationImpl tc;

  public InjectorImpl(ConfigurationImpl old_tc) throws BindException {
    tc = new ConfigurationBuilderImpl(old_tc).build();
  }

  @SuppressWarnings("unchecked")
  private InjectionPlan<?> wrapInjectionPlans(Node infeasibleNode,
      List<InjectionPlan<?>> list, boolean forceAmbiguous) {
    if (list.size() == 0) {
      return new InjectionPlan.Subplan<>(infeasibleNode);
    } else if ((!forceAmbiguous) && list.size() == 1) {
      return list.get(0);
    } else {
      return new InjectionPlan.Subplan<>(infeasibleNode,
          list.toArray(new InjectionPlan[0]));
    }
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private void buildInjectionPlan(final String name,
      Map<String, InjectionPlan<?>> memo) {
    if (memo.containsKey(name)) {
      if (InjectionPlan.BUILDING == memo.get(name)) {
        throw new IllegalStateException("Detected loopy constructor involving "
            + name);
      } else {
        return;
      }
    }
    memo.put(name, InjectionPlan.BUILDING);
    final Node n; // TODO: Register the node here (to bring into line with
            // bindVolatile(...)
    try {
      n = tc.builder.namespace.getNode(name);
    } catch (NameResolutionException e) {
      throw new IllegalArgumentException("Unregistered class " + name, e);
    }
    final InjectionPlan<?> ip;
    if (n instanceof NamedParameterNode) {
      NamedParameterNode<?> np = (NamedParameterNode<?>) n;
      Object instance = tc.builder.namedParameterInstances.get(n);
      if (instance == null) {
        // Arguably, we should instantiate default instances in InjectorImpl
        // (instead of in ClassHierarchy), but we want ClassHierarchy to check
        // that the default string parses correctly.
        instance = tc.builder.namespace.defaultNamedParameterInstances.get(n);
      }
      if(instance instanceof Class) {
        String implName = ((Class) instance).getName();
        buildInjectionPlan(implName, memo);
        ip = new InjectionPlan.Subplan<>(np, 0, memo.get(implName));
      } else {
        ip = new InjectionPlan.Instance<Object>(np, instance);
      }
    } else if (n instanceof ClassNode) {
      ClassNode<?> cn = (ClassNode<?>) n;
      if (tc.builder.singletonInstances.containsKey(cn)) {
        ip = new InjectionPlan.Instance<Object>(cn, tc.builder.singletonInstances.get(cn));
      } else if (tc.builder.boundConstructors.containsKey(cn)) {
        String constructorName = tc.builder.boundConstructors.get(cn).getFullName();
        buildInjectionPlan(constructorName, memo);
        ip = new InjectionPlan.Subplan(cn, 0, memo.get(constructorName));
        memo.put(cn.getFullName(), ip);
        // ip = new Instance(cn, null);
      } else if (tc.builder.boundImpls.containsKey(cn)
          && !(tc.builder.boundImpls.get(cn).getFullName().equals(cn.getFullName()))) {
        String implName = tc.builder.boundImpls.get(cn).getFullName();
        buildInjectionPlan(implName, memo);
        ip = new InjectionPlan.Subplan(cn, 0, memo.get(implName));
        memo.put(cn.getFullName(), ip);
      } else {
        List<ClassNode<?>> classNodes = new ArrayList<>();
        if (tc.builder.boundImpls.get(cn) == null) {
          // if we're here, and there is a bound impl, then we're bound to
          // ourselves,
          // so skip this loop.
          for (ClassNode<?> c : tc.builder.namespace.getKnownImpls(cn)) {
            classNodes.add(c);
          }
        }
        classNodes.add(cn);
        List<InjectionPlan<?>> sub_ips = new ArrayList<InjectionPlan<?>>();
        for (ClassNode<?> thisCN : classNodes) {
          final List<InjectionPlan<?>> constructors = new ArrayList<InjectionPlan<?>>();
          final List<ConstructorDef<?>> constructorList = new ArrayList<>();
          if (tc.builder.legacyConstructors.containsKey(thisCN)) {
            constructorList.add(tc.builder.legacyConstructors.get(thisCN));
          }
          constructorList.addAll(Arrays.asList(thisCN.getInjectableConstructors()));

          for (ConstructorDef<?> def : constructorList) {
            List<InjectionPlan<?>> args = new ArrayList<InjectionPlan<?>>();
            for (ConstructorArg arg : def.getArgs()) {
              String argName = arg.getName(); // getFullyQualifiedName(thisCN.clazz);
              buildInjectionPlan(argName, memo);
              args.add(memo.get(argName));
            }
            InjectionPlan.Constructor constructor = new InjectionPlan.Constructor(
                thisCN, def, args.toArray(new InjectionPlan[0]));
            constructors.add(constructor);
          }
          sub_ips
              .add(wrapInjectionPlans(thisCN, constructors, false));
        }
        if (classNodes.size() == 1
            && classNodes.get(0).getFullName()/*getClazz().getName()*/.equals(name)) {
          ip = wrapInjectionPlans(n, sub_ips, false);
        } else {
          ip = wrapInjectionPlans(n, sub_ips, true);
        }
      }
    } else if (n instanceof PackageNode) {
      throw new IllegalArgumentException(
          "Request to instantiate Java package as object");
    } else if (n instanceof NamespaceNode) {
      throw new IllegalArgumentException(
          "Request to instantiate ConfigurationBuilderImpl namespace as object");
    } else {
      throw new IllegalStateException(
          "Type hierarchy contained unknown node type!:" + n);
    }
    memo.put(name, ip);
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
  public InjectionPlan<?> getInjectionPlan(String name) {
    Map<String, InjectionPlan<?>> memo = new HashMap<String, InjectionPlan<?>>();
    buildInjectionPlan(name, memo);
    return memo.get(name);
  }

  @SuppressWarnings("unchecked")
  public <T> InjectionPlan<T> getInjectionPlan(Class<T> name) {
    return (InjectionPlan<T>) getInjectionPlan(name.getName());
  }

  @Override
  public boolean isInjectable(String name) throws BindException {
    InjectionPlan<?> p = getInjectionPlan(name);
    return p.isInjectable();
  }

  @Override
  public boolean isInjectable(Class<?> clazz) throws BindException {
    return isInjectable(clazz.getName());
  }

  @Override
  public boolean isParameterSet(String name) throws BindException {
    InjectionPlan<?> p = getInjectionPlan(name);
    return p.isInjectable();
  }

  @Override
  public boolean isParameterSet(Class<? extends Name<?>> name)
      throws BindException {
    return isParameterSet(name.getName());
  }

  boolean populated = false;

  private void populateSingletons() throws InjectionException {
    if (!populated) {
      populated = true;
      boolean stillHope = true;
      boolean allSucceeded = false;
      while (!allSucceeded) {
        boolean oneSucceeded = false;
        allSucceeded = true;
        for (ClassNode<?> cn : tc.builder.singletons) {
          if (!tc.builder.singletonInstances.containsKey(cn)) {
            try {
              getInstance(cn.getFullName());//getClazz());
              // System.err.println("success " + cn);
              oneSucceeded = true;
            } catch (SingletonInjectionException e) {
              // System.err.println("failure " + cn);
              allSucceeded = false;
              if (!stillHope) {
                throw e;
              }
            }
          }
        }
        if (!oneSucceeded) {
          stillHope = false;
        }
      }
    }
  }

  @Override
  public <U> U getInstance(Class<U> clazz) throws InjectionException {
    populateSingletons();
    InjectionPlan<U> plan = getInjectionPlan(clazz);
    return injectFromPlan(plan);
  }
  @Override
  public <U> U getNamedInstance(Class<? extends Name<U>> clazz) throws InjectionException {
    populateSingletons();
    @SuppressWarnings("unchecked")
    InjectionPlan<U> plan = (InjectionPlan<U>) getInjectionPlan(clazz.getName());
    return injectFromPlan(plan);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <U> U getInstance(String clazz) throws InjectionException {
    populateSingletons();
    InjectionPlan<?> plan = getInjectionPlan(clazz);
    return (U) injectFromPlan(plan);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T getNamedParameter(Class<? extends Name<T>> clazz)
      throws InjectionException {
    InjectionPlan<T> plan = (InjectionPlan<T>) getInjectionPlan(clazz.getName());
    return (T) injectFromPlan(plan);
  }
  private <T> Constructor<T> getConstructor(ConstructorDef<T> constructor) throws ClassNotFoundException, NoSuchMethodException, SecurityException {
    @SuppressWarnings("unchecked")
    Class<T> clazz = (Class<T>)tc.builder.namespace.classForName(constructor.getClassName());
    ConstructorArg[] args = constructor.getArgs();
    Class<?> parameterTypes[] = new Class[args.length];
    for(int i = 0; i < args.length; i++) {
      parameterTypes[i] = tc.builder.namespace.classForName(args[i].getType());
    }
    Constructor<T> cons = clazz.getDeclaredConstructor(parameterTypes);
    cons.setAccessible(true);
    return cons;
  }
  @SuppressWarnings("unchecked")
  <T> T injectFromPlan(InjectionPlan<T> plan) throws InjectionException {
    if (!plan.isFeasible()) {
      throw new InjectionException("Attempt to inject infeasible plan: "
          + plan.toPrettyString());
    }
    if (plan.isAmbiguous()) {
      throw new IllegalArgumentException("Attempt to inject ambiguous plan: "
          + plan.toPrettyString());
    }
    if (plan instanceof InjectionPlan.Instance) {
      return ((InjectionPlan.Instance<T>) plan).instance;
    } else if (plan instanceof InjectionPlan.Constructor) {
      InjectionPlan.Constructor<T> constructor = (InjectionPlan.Constructor<T>) plan;
      if (tc.builder.singletonInstances.containsKey(constructor.getNode())) {
        throw new SingletonInjectionException(
            "Attempt to re-instantiate singleton: " + constructor.getNode());
      }
      Object[] args = new Object[constructor.args.length];
      for (int i = 0; i < constructor.args.length; i++) {
        args[i] = injectFromPlan(constructor.args[i]);
      }
      try {
        T ret = getConstructor((ConstructorDef<T>)constructor.constructor).newInstance(args);
        if (tc.builder.singletons.contains(constructor.getNode())) {
          tc.builder.singletonInstances.put(constructor.getNode(), ret);
        }
        // System.err.println("returning a new " + constructor.getNode());
        return ret;
      } catch (ReflectiveOperationException e) {
        throw new InjectionException("Could not invoke constructor", e);
      }
    } else if (plan instanceof InjectionPlan.Subplan) {
      InjectionPlan.Subplan<T> ambiguous = (InjectionPlan.Subplan<T>) plan;
      if(ambiguous.isInjectable()) {
        if (tc.builder.singletonInstances.containsKey(ambiguous.getNode())) {
          throw new SingletonInjectionException(
              "Attempt to re-instantiate singleton: " + ambiguous.getNode());
        }
        Object ret = injectFromPlan(ambiguous.getDelegatedPlan());
        if (tc.builder.singletons.contains(ambiguous.getNode())) {
          // Cast is safe since singletons is of type Set<ClassNode<?>>
          tc.builder.singletonInstances.put((ClassNode<?>)ambiguous.getNode(), ret);
        }
        // TODO: Check "T" in "instanceof ExternalConstructor<T>"
        if (ret instanceof ExternalConstructor) {
          // TODO fix up generic types for injectFromPlan with external
          // constructor!
          return ((ExternalConstructor<T>) ret).newInstance();
        } else {
          return (T) ret;
        }
      } else {
        if(ambiguous.getNumAlternatives() == 0) {
          throw new InjectionException("Attempt to inject infeasible plan:"
              + plan.toPrettyString());
        } else {
          throw new InjectionException("Attempt to inject ambiguous plan:"
              + plan.toPrettyString());
        }
      }
    } else {
      throw new IllegalStateException("Unknown plan type: " + plan);
    }
  }

  private static InjectorImpl copy(InjectorImpl old,
      Configuration... configurations) {
    final InjectorImpl i;
    try {
      final ConfigurationBuilderImpl cb = new ConfigurationBuilderImpl(old.tc);
      for (Configuration c : configurations) {
        cb.addConfiguration(c);
      }
      i = new InjectorImpl(cb.build());
    } catch (BindException e) {
      throw new IllegalStateException(
          "Unexpected error copying configuration!", e);
    }
    return i;
  }

  @Override
  public <T> void bindVolatileInstance(Class<T> c, T o) throws BindException {
    bindVolatileInstanceNoCopy(c, o);
  }

  @Override
  public <T> void bindVolatileParameter(Class<? extends Name<T>> c, T o)
      throws BindException {
    bindVolatileParameterNoCopy(c, o);
  }

  <T> void bindVolatileInstanceNoCopy(Class<T> c, T o) throws BindException {
    Node n = tc.builder.namespace.register(ReflectionUtilities.getFullName(c));
    /*
     * try { n = tc.namespace.getNode(c); } catch (NameResolutionException e) {
     * // TODO: Unit test for bindVolatileInstance to unknown class. throw new
     * BindException("Can't bind to unknown class " + c.getName(), e); }
     */

    if (n instanceof ClassNode) {
      ClassNode<?> cn = (ClassNode<?>) n;
      Object old = tc.builder.singletonInstances.get(cn);
      if (old != null) {
        throw new BindException("Attempt to re-bind singleton.  Old value was "
            + old + " new value is " + o);
      }
      tc.builder.singletonInstances.put(cn, o);
    } else {
      throw new IllegalArgumentException("Expected Class but got " + c
          + " (probably a named parameter).");
    }
  }

  <T> void bindVolatileParameterNoCopy(Class<? extends Name<T>> c, T o)
      throws BindException {
    Node n = tc.builder.namespace.register(ReflectionUtilities.getFullName(c));
    if (n instanceof NamedParameterNode) {
      NamedParameterNode<?> np = (NamedParameterNode<?>) n;
      Object old = tc.builder.namedParameterInstances.get(np);
      if (old != null) {
        throw new BindException(
            "Attempt to re-bind named parameter.  Old value was " + old
                + " new value is " + o);
      }
      tc.builder.namedParameterInstances.put(np, o);
      if(o instanceof Class) {
        tc.builder.namespace.register(ReflectionUtilities.getFullName((Class<?>) o));
      }
    } else {
      throw new IllegalArgumentException("Expected Name, got " + c
          + " (probably a class)");
    }
  }

  @Override
  public Injector createChildInjector(Configuration... configurations)
      throws BindException {
    InjectorImpl ret;
    ret = copy(this, configurations);
    return ret;
  }

}