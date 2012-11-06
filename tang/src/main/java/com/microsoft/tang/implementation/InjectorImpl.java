package com.microsoft.tang.implementation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.microsoft.tang.Configuration;
import com.microsoft.tang.Injector;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.exceptions.NameResolutionException;
import com.microsoft.tang.implementation.InjectionPlan.InfeasibleInjectionPlan;
import com.microsoft.tang.implementation.InjectionPlan.Instance;
import com.microsoft.tang.implementation.InjectionPlan.AmbiguousInjectionPlan;
import com.microsoft.tang.implementation.TypeHierarchy.ClassNode;
import com.microsoft.tang.implementation.TypeHierarchy.ConstructorArg;
import com.microsoft.tang.implementation.TypeHierarchy.ConstructorDef;
import com.microsoft.tang.implementation.TypeHierarchy.NamedParameterNode;
import com.microsoft.tang.implementation.TypeHierarchy.NamespaceNode;
import com.microsoft.tang.implementation.TypeHierarchy.Node;
import com.microsoft.tang.implementation.TypeHierarchy.PackageNode;

public class InjectorImpl implements Injector {
  private final ConfigurationImpl tc;

  public InjectorImpl(ConfigurationImpl old_tc)
      throws InjectionException, BindException {
    tc = new ConfigurationBuilderImpl(old_tc).build();
    for (ClassNode<?> cn : tc.singletons) {
      if(!tc.singletonInstances.containsKey(cn)) {
        Object o = getInstance(cn.getClazz());
        tc.singletonInstances.put(cn, o);
      }
    }
  }

  private InjectionPlan<?> wrapInjectionPlans(String infeasibleName,
      List<InjectionPlan<?>> list, boolean forceAmbiguous) {
    if (list.size() == 0) {
      return new InfeasibleInjectionPlan<Object>(infeasibleName);
    } else if ((!forceAmbiguous) && list.size() == 1) {
      return list.get(0);
    } else {
      InjectionPlan<?>[] injectionPlans = (InjectionPlan<?>[]) new InjectionPlan[0];
      return new InjectionPlan.AmbiguousInjectionPlan<Object>(
          list.toArray(injectionPlans));
    }
  }

  private void buildInjectionPlan(String name,
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
    Node n;
    try {
      n = tc.namespace.getNode(name);
    } catch (NameResolutionException e) {
      throw new IllegalArgumentException("Unregistered class " + name, e);
    }
    final InjectionPlan<?> ip;
    if (n instanceof NamedParameterNode) {
      NamedParameterNode<?> np = (NamedParameterNode<?>) n;
      Object instance = tc.namedParameterInstances.get(n);
      if (instance == null) {
        instance = np.defaultInstance;
      }
      ip = new Instance<Object>(np, instance);
    } else if (n instanceof ClassNode) {
      ClassNode<?> cn = (ClassNode<?>) n;
      if (tc.singletonInstances.containsKey(cn)) {
        ip = new Instance<Object>(cn, tc.singletonInstances.get(cn));
      } else if (tc.boundConstructors.containsKey(cn)) {
        throw new UnsupportedOperationException("Vivifiers aren't working yet!");
        // ip = new Instance(cn, null);
      } else if (tc.boundImpls.containsKey(cn)) {
        String implName = tc.boundImpls.get(cn).getName();
        buildInjectionPlan(implName, memo);
        ip = memo.get(implName);
      } else {
        List<ClassNode<?>> classNodes = new ArrayList<ClassNode<?>>();
        for (ClassNode<?> c : tc.namespace.getKnownImpls(cn)) {
          classNodes.add(c);
        }
        classNodes.add(cn);
        List<InjectionPlan<?>> sub_ips = new ArrayList<InjectionPlan<?>>();
        for (ClassNode<?> thisCN : classNodes) {
          List<InjectionPlan<?>> constructors = new ArrayList<InjectionPlan<?>>();
          for (ConstructorDef<?> def : thisCN.injectableConstructors) {
            List<InjectionPlan<?>> args = new ArrayList<InjectionPlan<?>>();
            for (ConstructorArg arg : def.args) {
              String argName = arg.getName(); // getFullyQualifiedName(thisCN.clazz);
              buildInjectionPlan(argName, memo);
              args.add(memo.get(argName));
            }
            @SuppressWarnings({ "rawtypes", "unchecked" })
            InjectionPlan.Constructor constructor = new InjectionPlan.Constructor(
                def, args.toArray(new InjectionPlan[0]));
            constructors.add(constructor);
          }
          sub_ips
              .add(wrapInjectionPlans(thisCN.getName(), constructors, false));
        }
        if (classNodes.size() == 1
            && classNodes.get(0).getClazz().getName().equals(name)) {
          ip = wrapInjectionPlans(name, sub_ips, false);
        } else {
          ip = wrapInjectionPlans(name, sub_ips, true);
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

  /**
   * Returns true if ConfigurationBuilderImpl is ready to instantiate the object
   * named by name.
   * 
   * @param name
   * @return
   * @throws NameResolutionException
   */
  public boolean isInjectable(String name) throws BindException {
    InjectionPlan<?> p = getInjectionPlan(name);
    return p.isInjectable();
  }

  @Override
  public <U> U getInstance(Class<U> clazz) throws InjectionException {
    InjectionPlan<U> plan = getInjectionPlan(clazz);
    return injectFromPlan(plan);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T getNamedParameter(Class<? extends Name<T>> clazz)
      throws InjectionException {
    InjectionPlan<T> plan = (InjectionPlan<T>) getInjectionPlan(clazz.getName());
    return (T) injectFromPlan(plan);
  }

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
      Object[] args = new Object[constructor.args.length];
      for (int i = 0; i < constructor.args.length; i++) {
        args[i] = injectFromPlan(constructor.args[i]);
      }
      try {
        return constructor.constructor.constructor.newInstance(args);
      } catch (ReflectiveOperationException e) {
        throw new InjectionException("Could not invoke constructor", e);
      }
    } else if (plan instanceof AmbiguousInjectionPlan) {
      AmbiguousInjectionPlan<T> ambiguous = (AmbiguousInjectionPlan<T>) plan;
      for (InjectionPlan<? extends T> p : ambiguous.alternatives) {
        if (p.isInjectable() && !p.isAmbiguous()) {
          return injectFromPlan(p);
        }
      }
      throw new IllegalStateException(
          "Thought there was an injectable plan, but can't find it!");
    } else if (plan instanceof InfeasibleInjectionPlan) {
      throw new InjectionException("Attempt to inject infeasible plan:"
          + plan.toPrettyString());
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
    } catch (BindException | InjectionException e) {
      throw new IllegalStateException(
          "Unexpected error copying configuration!", e);
    }
    return i;
  }


  @Override
  public <T> InjectorImpl bindVolatileInstance(Class<T> c, T o)
      throws BindException {
    InjectorImpl ret = copy(this);
    ret.bindVolatileInstanceNoCopy(c, o);
    return ret;
  }

  @Override
  public <T> InjectorImpl bindVolatileParameter(Class<? extends Name<T>> c, T o)
      throws BindException {
    InjectorImpl ret = copy(this);
    ret.bindVolatileParameterNoCopy(c, o);
    return ret;
  }

  <T> void bindVolatileInstanceNoCopy(Class<T> c, T o)
      throws BindException {
    Node n;
    try {
      n = tc.namespace.getNode(c);
    } catch (NameResolutionException e) {
      // TODO: Unit test for bindVolatileInstance to unknown class.
      throw new BindException("Can't bind to unknown class " + c, e);
    }

    if (n instanceof ClassNode) {
      ClassNode<?> cn = (ClassNode<?>) n;
      cn.setIsSingleton();
      Object old = tc.singletonInstances.get(cn);
      if (old != null) {
        throw new BindException("Attempt to re-bind singleton.  Old value was "
            + old + " new value is " + o);
      }
      tc.singletonInstances.put(cn, o);
    } else {
      throw new IllegalArgumentException("Expected Class but got " + c
          + " (probably a named parameter).");
    }
  }

  <T> void bindVolatileParameterNoCopy(Class<? extends Name<T>> c, T o)
      throws BindException {
    Node n;
    try {
      n = tc.namespace.getNode(c);
    } catch (NameResolutionException e) {
      throw new BindException("Can't bind to unknown name " + c, e);
    }
    if (n instanceof NamedParameterNode) {
      NamedParameterNode<?> np = (NamedParameterNode<?>) n;
      Object old = tc.namedParameterInstances.get(np);
      if (old != null) {
        throw new BindException(
            "Attempt to re-bind named parameter.  Old value was " + old
                + " new value is " + o);
      }
      tc.namedParameterInstances.put(np, o);
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