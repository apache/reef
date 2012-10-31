package com.microsoft.tang.implementation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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

  public InjectorImpl(ConfigurationImpl tc) {
    this.tc = tc;
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
      throw new IllegalArgumentException("Unregistered class" + name, e);
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
    if (!tc.sealed) {
      tc.sealed = true;
      for (ClassNode<?> cn : tc.singletons) {
        Object o = getInstance(cn.getClazz());
        tc.singletonInstances.put(cn, o);
      }
    }
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
      throw new IllegalArgumentException("Attempt to inject infeasible plan: "
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
      throw new IllegalArgumentException("Attempt to inject infeasible plan!");
    } else {
      throw new IllegalStateException("Unknown plan type: " + plan);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> InjectorImpl bindVolatileInstance(Class<T> c, T o)
      throws InjectionException {
    tc.dirtyBit = true;
    Node n;
    try {
      n = tc.namespace.getNode(c);
    } catch(NameResolutionException e) {
      // TODO Unit test for bindVolatileInstance to unknown class.
      throw new InjectionException("Can't bind to unknown class " + c, e);
    }
    if (n instanceof NamedParameterNode) {
      NamedParameterNode<T> np = (NamedParameterNode<T>) n;
      tc.namedParameterInstances.put(np, o);
      ClassNode<T> cn = (ClassNode<T>) n;
      cn.setIsSingleton();
      tc.singletonInstances.put(cn, o);
    } else {
      throw new IllegalArgumentException(
          "Expected Class or NamedParameter, but " + c + " is neither.");
    }
    throw new UnsupportedOperationException(
        "Need to update bindVolatileInstance for the new API");
  }

  @Override
  public Injector createChildInjector(Configuration... configurations)
      throws BindException {
    ConfigurationBuilderImpl cb = new ConfigurationBuilderImpl();
    try {
      cb.addConfiguration(this.tc);
    } catch (BindException e) {
      throw new IllegalStateException(
          "Hit BindException when copying configuration.  Can't happen.", e);
    }
    for (Configuration c : configurations) {
      cb.addConfiguration(c);
    }
    InjectorImpl i = new InjectorImpl(cb.build());
    for (Entry<ClassNode<?>, Object> e : tc.singletonInstances.entrySet()) {
      @SuppressWarnings("unchecked")
      Class<Object> clazz = (Class<Object>) e.getKey().getClazz();
      try {
        i.bindVolatileInstance(clazz, e.getValue());
      } catch(InjectionException f) {
        throw new IllegalStateException("Could not copy reference to volatile instance.", f.getCause());
      }
    }
    return i;
  }

}