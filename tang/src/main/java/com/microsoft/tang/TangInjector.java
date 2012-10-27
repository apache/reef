package com.microsoft.tang;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.microsoft.tang.InjectionPlan.AmbiguousInjectionPlan;
import com.microsoft.tang.InjectionPlan.InfeasibleInjectionPlan;
import com.microsoft.tang.InjectionPlan.Instance;
import com.microsoft.tang.TypeHierarchy.ClassNode;
import com.microsoft.tang.TypeHierarchy.ConstructorArg;
import com.microsoft.tang.TypeHierarchy.ConstructorDef;
import com.microsoft.tang.TypeHierarchy.NamedParameterNode;
import com.microsoft.tang.TypeHierarchy.NamespaceNode;
import com.microsoft.tang.TypeHierarchy.Node;
import com.microsoft.tang.TypeHierarchy.PackageNode;
import com.microsoft.tang.exceptions.NameResolutionException;

public class TangInjector {
  private final TangConf tc;
  
  public TangInjector(TangConf tc) {
    this.tc = tc;
  }
  private InjectionPlan wrapInjectionPlans(String infeasibleName,
      List<? extends InjectionPlan> list) {
    if (list.size() == 0) {
      return new InfeasibleInjectionPlan(infeasibleName);
    } else if (list.size() == 1) {
      return list.get(0);
    } else {
      return new InjectionPlan.AmbiguousInjectionPlan(
          list.toArray(new InjectionPlan[0]));
    }
  }

  private void buildInjectionPlan(String name,
      Map<String, InjectionPlan> memo) throws NameResolutionException {
    if (memo.containsKey(name)) {
      if (InjectionPlan.BUILDING == memo.get(name)) {
        throw new IllegalStateException(
            "Detected loopy constructor involving " + name);
      } else {
        return;
      }
    }
    memo.put(name, InjectionPlan.BUILDING);

    Node n = tc.tang.namespace.getNode(name);
    final InjectionPlan ip;
    if (n instanceof NamedParameterNode) {
      NamedParameterNode<?> np = (NamedParameterNode<?>) n;
      Object instance = tc.tang.namedParameterInstances.get(n);
      if (instance == null) {
        instance = np.defaultInstance;
      }
      ip = new Instance(np, instance);
    } else if (n instanceof ClassNode) {
      ClassNode<?> cn = (ClassNode<?>) n;
      if (tc.tang.singletonInstances.containsKey(cn)) {
        ip = new Instance(cn, tc.tang.singletonInstances.get(cn));
      } else if (tc.tang.constructors.containsKey(cn)) {
        throw new UnsupportedOperationException(
            "Vivifiers aren't working yet!");
        // ip = new Instance(cn, null);
      } else if (tc.tang.defaultImpls.containsKey(cn)) {
        String implName = tc.tang.defaultImpls.get(cn).getName();
        buildInjectionPlan(implName, memo);
        ip = memo.get(implName);
      } else {
        List<ClassNode<?>> classNodes = new ArrayList<ClassNode<?>>();
        for (ClassNode<?> c : tc.tang.namespace.getKnownImpls(cn)) {
          classNodes.add(c);
        }
        classNodes.add(cn);
        List<InjectionPlan> sub_ips = new ArrayList<InjectionPlan>();
        for (ClassNode<?> thisCN : classNodes) {
          List<InjectionPlan.Constructor> constructors = new ArrayList<InjectionPlan.Constructor>();
          for (ConstructorDef def : thisCN.injectableConstructors) {
            List<InjectionPlan> args = new ArrayList<InjectionPlan>();
            for (ConstructorArg arg : def.args) {
              String argName = arg.getName(); // getFullyQualifiedName(thisCN.clazz);
              buildInjectionPlan(argName, memo);
              args.add(memo.get(argName));
            }
            constructors.add(new InjectionPlan.Constructor(def, args
                .toArray(new InjectionPlan[0])));
          }
          sub_ips.add(wrapInjectionPlans(thisCN.getName(), constructors));
        }
        ip = wrapInjectionPlans(name, sub_ips);
      }
    } else if (n instanceof PackageNode) {
      throw new IllegalArgumentException(
          "Request to instantiate Java package as object");
    } else if (n instanceof NamespaceNode) {
      throw new IllegalArgumentException(
          "Request to instantiate Tang namespace as object");
    } else {
      throw new IllegalStateException(
          "Type hierarchy contained unknown node type!:" + n);
    }
    memo.put(name, ip);
  }

  /**
   * Return an injection plan for the given class / parameter name. This
   * will be more useful once plans can be serialized / deserialized /
   * pretty printed.
   * 
   * @param name
   *          The name of an injectable class or interface, or a
   *          NamedParameter.
   * @return
   * @throws NameResolutionException
   */
  public InjectionPlan getInjectionPlan(String name)
      throws NameResolutionException {
    Map<String, InjectionPlan> memo = new HashMap<String, InjectionPlan>();
    buildInjectionPlan(name, memo);
    return memo.get(name);
  }

  /**
   * Returns true if Tang is ready to instantiate the object named by name.
   * 
   * @param name
   * @return
   * @throws NameResolutionException
   */
  public boolean isInjectable(String name) throws NameResolutionException {
    InjectionPlan p = getInjectionPlan(name);
    return p.isInjectable();
  }

  /**
   * Get a new instance of the class clazz.
   * 
   * @param clazz
   * @return
   * @throws NameResolutionException
   * @throws ReflectiveOperationException
   */
  @SuppressWarnings("unchecked")
  public <U> U getInstance(Class<U> clazz) throws NameResolutionException,
      ReflectiveOperationException {
    tc.tang.register(clazz);
    if (!tc.tang.sealed) {
      tc.tang.sealed = true;
      for (ClassNode<?> cn : tc.tang.singletons) {
        Object o = getInstance(cn.getClazz());
        tc.tang.singletonInstances.put(cn, o);
      }
    }
    InjectionPlan plan = getInjectionPlan(clazz.getName());
    return (U) injectFromPlan(plan);
  }

  Object injectFromPlan(InjectionPlan plan)
      throws ReflectiveOperationException {
    if (plan.getNumAlternatives() == 0) {
      throw new IllegalArgumentException(
          "Attempt to inject infeasible plan: " + plan.toPrettyString());
    }
    if (plan.getNumAlternatives() > 1) {
      throw new IllegalArgumentException(
          "Attempt to inject ambiguous plan: " + plan.toPrettyString());
    }
    if (plan instanceof InjectionPlan.Instance) {
      return ((InjectionPlan.Instance) plan).instance;
    } else if (plan instanceof InjectionPlan.Constructor) {
      InjectionPlan.Constructor constructor = (InjectionPlan.Constructor) plan;
      Object[] args = new Object[constructor.args.length];
      for (int i = 0; i < constructor.args.length; i++) {
        args[i] = injectFromPlan(constructor.args[i]);
      }
      return constructor.constructor.constructor.newInstance(args);
    } else if (plan instanceof AmbiguousInjectionPlan) {
      AmbiguousInjectionPlan ambiguous = (AmbiguousInjectionPlan) plan;
      for (InjectionPlan p : ambiguous.alternatives) {
        if (p.isInjectable()) {
          return injectFromPlan(p);
        }
      }
      throw new IllegalStateException(
          "Thought there was an injectable plan, but can't find it!");
    } else if (plan instanceof InfeasibleInjectionPlan) {
      throw new IllegalArgumentException(
          "Attempt to inject infeasible plan!");
    } else {
      throw new IllegalStateException("Unknown plan type: " + plan);
    }
  }
  /**
   * Note: if you call this method, then you may no longer serialize this tang
   * to a config file.
   * 
   * @param c
   * @param o
   * @throws NameResolutionException
   */
  @SuppressWarnings("unchecked")
  public <T> void bindVolatialeInstance(Class<T> c, T o)
      throws NameResolutionException {
    tc.tang.dirtyBit = true;
    tc.tang.register(c);
    Node n = tc.tang.namespace.getNode(c);
    if (n instanceof NamedParameterNode) {
      NamedParameterNode<T> np = (NamedParameterNode<T>) n;
      tc.tang.namedParameterInstances.put(np, o);
      ClassNode<T> cn = (ClassNode<T>) n;
      cn.setIsSingleton();
      tc.tang.singletonInstances.put(cn, o);
    } else {
      throw new IllegalArgumentException(
          "Expected Class or NamedParameter, but " + c + " is neither.");
    }
  }

}