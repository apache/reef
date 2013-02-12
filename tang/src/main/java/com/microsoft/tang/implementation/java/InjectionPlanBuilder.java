package com.microsoft.tang.implementation.java;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.microsoft.tang.ClassNode;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.ConstructorArg;
import com.microsoft.tang.ConstructorDef;
import com.microsoft.tang.NamedParameterNode;
import com.microsoft.tang.NamespaceNode;
import com.microsoft.tang.Node;
import com.microsoft.tang.PackageNode;
import com.microsoft.tang.exceptions.NameResolutionException;
import com.microsoft.tang.implementation.Constructor;
import com.microsoft.tang.implementation.InjectionPlan;
import com.microsoft.tang.implementation.Subplan;

public class InjectionPlanBuilder {
  ConfigurationBuilderImpl cb;
  public InjectionPlanBuilder(Configuration conf) {
    this.cb = ((ConfigurationImpl)conf).builder;
  }
  static final InjectionPlan<?> BUILDING = new InjectionPlan<Object>(null) {
    @Override
    public int getNumAlternatives() {
      throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
      return "BUILDING INJECTION PLAN";
    }

    @Override
    public boolean isAmbiguous() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isInjectable() {
      throw new UnsupportedOperationException();
    }
  };

  @SuppressWarnings("unchecked")
  private InjectionPlan<?> wrapInjectionPlans(Node infeasibleNode,
      List<InjectionPlan<?>> list, boolean forceAmbiguous) {
    if (list.size() == 0) {
      return new Subplan<>(infeasibleNode);
    } else if ((!forceAmbiguous) && list.size() == 1) {
      return list.get(0);
    } else {
      return new Subplan<>(infeasibleNode, list.toArray(new InjectionPlan[0]));
    }
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private void buildInjectionPlan(final String name,
      Map<String, InjectionPlan<?>> memo) {
    if (memo.containsKey(name)) {
      if (BUILDING == memo.get(name)) {
        throw new IllegalStateException("Detected loopy constructor involving "
            + name);
      } else {
        return;
      }
    }
    memo.put(name, BUILDING);
    final Node n; // TODO: Register the node here (to bring into line with
    // bindVolatile(...)
    try {
      n = cb.namespace.getNode(name);
    } catch (NameResolutionException e) {
      throw new IllegalArgumentException("Unregistered class " + name, e);
    }
    final InjectionPlan<?> ip;
    if (n instanceof NamedParameterNode) {
      NamedParameterNode<?> np = (NamedParameterNode<?>) n;
      Object instance = cb.namedParameterInstances.get(n);
      if (instance == null) {
        // Arguably, we should instantiate default instances in InjectorImpl
        // (instead of in ClassHierarchy), but we want ClassHierarchy to check
        // that the default string parses correctly.
        instance = cb.namespace.defaultNamedParameterInstances.get(n);
      }
      if (instance instanceof Class) {
        String implName = ((Class) instance).getName();
        buildInjectionPlan(implName, memo);
        ip = new Subplan<>(np, 0, memo.get(implName));
      } else {
        ip = new JavaInstance<Object>(np, instance);
      }
    } else if (n instanceof ClassNode) {
      ClassNode<?> cn = (ClassNode<?>) n;
      if (cb.singletonInstances.containsKey(cn)) {
        ip = new JavaInstance<Object>(cn, cb.singletonInstances.get(cn));
      } else if (cb.boundConstructors.containsKey(cn)) {
        String constructorName = cb.boundConstructors.get(cn).getFullName();
        buildInjectionPlan(constructorName, memo);
        ip = new Subplan(cn, 0, memo.get(constructorName));
        memo.put(cn.getFullName(), ip);
        // ip = new Instance(cn, null);
      } else if (cb.boundImpls.containsKey(cn)
          && !(cb.boundImpls.get(cn).getFullName().equals(cn.getFullName()))) {
        String implName = cb.boundImpls.get(cn).getFullName();
        buildInjectionPlan(implName, memo);
        ip = new Subplan(cn, 0, memo.get(implName));
        memo.put(cn.getFullName(), ip);
      } else {
        List<ClassNode<?>> classNodes = new ArrayList<>();
        if (cb.boundImpls.get(cn) == null) {
          // if we're here, and there is a bound impl, then we're bound to
          // ourselves,
          // so skip this loop.
          for (ClassNode<?> c : cb.namespace.getKnownImpls(cn)) {
            classNodes.add(c);
          }
        }
        classNodes.add(cn);
        List<InjectionPlan<?>> sub_ips = new ArrayList<InjectionPlan<?>>();
        for (ClassNode<?> thisCN : classNodes) {
          final List<InjectionPlan<?>> constructors = new ArrayList<InjectionPlan<?>>();
          final List<ConstructorDef<?>> constructorList = new ArrayList<>();
          if (cb.legacyConstructors.containsKey(thisCN)) {
            constructorList.add(cb.legacyConstructors.get(thisCN));
          }
          constructorList.addAll(Arrays.asList(thisCN
              .getInjectableConstructors()));

          for (ConstructorDef<?> def : constructorList) {
            List<InjectionPlan<?>> args = new ArrayList<InjectionPlan<?>>();
            for (ConstructorArg arg : def.getArgs()) {
              String argName = arg.getName(); // getFullyQualifiedName(thisCN.clazz);
              buildInjectionPlan(argName, memo);
              args.add(memo.get(argName));
            }
            Constructor constructor = new Constructor(thisCN, def,
                args.toArray(new InjectionPlan[0]));
            constructors.add(constructor);
          }
          sub_ips.add(wrapInjectionPlans(thisCN, constructors, false));
        }
        if (classNodes.size() == 1 && classNodes.get(0).getFullName()
        /* getClazz().getName() */.equals(name)) {
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

}
