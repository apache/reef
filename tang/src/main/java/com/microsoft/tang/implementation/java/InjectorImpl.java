package com.microsoft.tang.implementation.java;

import com.microsoft.tang.ClassNode;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.ConstructorArg;
import com.microsoft.tang.ConstructorDef;
import com.microsoft.tang.ExternalConstructor;
import com.microsoft.tang.Injector;
import com.microsoft.tang.NamedParameterNode;
import com.microsoft.tang.Node;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.implementation.Constructor;
import com.microsoft.tang.implementation.InjectionPlan;
import com.microsoft.tang.implementation.Subplan;
import com.microsoft.tang.util.ReflectionUtilities;

public class InjectorImpl implements Injector {

  private class SingletonInjectionException extends InjectionException {
    private static final long serialVersionUID = 1L;

    SingletonInjectionException(String s) {
      super(s);
    }
  }

//  private final ConfigurationImpl tc;
  private final InjectionPlanBuilder ib;

  public InjectorImpl(ConfigurationImpl old_tc) throws BindException {
    this.ib = new InjectionPlanBuilder(new ConfigurationBuilderImpl(old_tc).build());
  }


  @Override
  public boolean isInjectable(String name) throws BindException {
    InjectionPlan<?> p = ib.getInjectionPlan(name);
    return p.isInjectable();
  }

  @Override
  public boolean isInjectable(Class<?> clazz) throws BindException {
    return isInjectable(clazz.getName());
  }

  @Override
  public boolean isParameterSet(String name) throws BindException {
    InjectionPlan<?> p = ib.getInjectionPlan(name);
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
        for (ClassNode<?> cn : ib.cb.singletons) {
          if (!ib.cb.singletonInstances.containsKey(cn)) {
            try {
              getInstance(cn.getFullName());// getClazz());
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
    InjectionPlan<U> plan = ib.getInjectionPlan(clazz);
    return injectFromPlan(plan);
  }

  @Override
  public <U> U getNamedInstance(Class<? extends Name<U>> clazz)
      throws InjectionException {
    populateSingletons();
    @SuppressWarnings("unchecked")
    InjectionPlan<U> plan = (InjectionPlan<U>) ib.getInjectionPlan(clazz.getName());
    return injectFromPlan(plan);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <U> U getInstance(String clazz) throws InjectionException {
    populateSingletons();
    InjectionPlan<?> plan = ib.getInjectionPlan(clazz);
    return (U) injectFromPlan(plan);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T getNamedParameter(Class<? extends Name<T>> clazz)
      throws InjectionException {
    InjectionPlan<T> plan = (InjectionPlan<T>) ib.getInjectionPlan(clazz.getName());
    return (T) injectFromPlan(plan);
  }

  private <T> java.lang.reflect.Constructor<T> getConstructor(ConstructorDef<T> constructor)
      throws ClassNotFoundException, NoSuchMethodException, SecurityException {
    @SuppressWarnings("unchecked")
    Class<T> clazz = (Class<T>) ib.cb.namespace.classForName(constructor
        .getClassName());
    ConstructorArg[] args = constructor.getArgs();
    Class<?> parameterTypes[] = new Class[args.length];
    for (int i = 0; i < args.length; i++) {
      parameterTypes[i] = ib.cb.namespace.classForName(args[i].getType());
    }
    java.lang.reflect.Constructor<T> cons = clazz.getDeclaredConstructor(parameterTypes);
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
    if (plan instanceof JavaInstance) {
      return ((JavaInstance<T>) plan).instance;
    } else if (plan instanceof Constructor) {
      Constructor<T> constructor = (Constructor<T>) plan;
      if (ib.cb.singletonInstances.containsKey(constructor.getNode())) {
        throw new SingletonInjectionException(
            "Attempt to re-instantiate singleton: " + constructor.getNode());
      }
      Object[] args = new Object[constructor.getArgs().length];
      for (int i = 0; i < constructor.getArgs().length; i++) {
        args[i] = injectFromPlan(constructor.getArgs()[i]);
      }
      try {
        T ret = getConstructor((ConstructorDef<T>) constructor.getConstructorDef())
            .newInstance(args);
        if (ib.cb.singletons.contains(constructor.getNode())) {
          ib.cb.singletonInstances.put(constructor.getNode(), ret);
        }
        // System.err.println("returning a new " + constructor.getNode());
        return ret;
      } catch (ReflectiveOperationException e) {
        throw new InjectionException("Could not invoke constructor", e);
      }
    } else if (plan instanceof Subplan) {
      Subplan<T> ambiguous = (Subplan<T>) plan;
      if (ambiguous.isInjectable()) {
        if (ib.cb.singletonInstances.containsKey(ambiguous.getNode())) {
          throw new SingletonInjectionException(
              "Attempt to re-instantiate singleton: " + ambiguous.getNode());
        }
        Object ret = injectFromPlan(ambiguous.getDelegatedPlan());
        if (ib.cb.singletons.contains(ambiguous.getNode())) {
          // Cast is safe since singletons is of type Set<ClassNode<?>>
          ib.cb.singletonInstances.put((ClassNode<?>) ambiguous.getNode(),
              ret);
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
        if (ambiguous.getNumAlternatives() == 0) {
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
      final ConfigurationBuilderImpl cb = new ConfigurationBuilderImpl(old.ib.cb);
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
    Node n = ib.cb.namespace.register(ReflectionUtilities.getFullName(c));
    /*
     * try { n = tc.namespace.getNode(c); } catch (NameResolutionException e) {
     * // TODO: Unit test for bindVolatileInstance to unknown class. throw new
     * BindException("Can't bind to unknown class " + c.getName(), e); }
     */

    if (n instanceof ClassNode) {
      ClassNode<?> cn = (ClassNode<?>) n;
      Object old = ib.cb.singletonInstances.get(cn);
      if (old != null) {
        throw new BindException("Attempt to re-bind singleton.  Old value was "
            + old + " new value is " + o);
      }
      ib.cb.singletonInstances.put(cn, o);
    } else {
      throw new IllegalArgumentException("Expected Class but got " + c
          + " (probably a named parameter).");
    }
  }

  <T> void bindVolatileParameterNoCopy(Class<? extends Name<T>> c, T o)
      throws BindException {
    Node n = ib.cb.namespace.register(ReflectionUtilities.getFullName(c));
    if (n instanceof NamedParameterNode) {
      NamedParameterNode<?> np = (NamedParameterNode<?>) n;
      Object old = ib.cb.namedParameterInstances.get(np);
      if (old != null) {
        throw new BindException(
            "Attempt to re-bind named parameter.  Old value was " + old
                + " new value is " + o);
      }
      ib.cb.namedParameterInstances.put(np, o);
      if (o instanceof Class) {
        ib.cb.namespace.register(ReflectionUtilities
            .getFullName((Class<?>) o));
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