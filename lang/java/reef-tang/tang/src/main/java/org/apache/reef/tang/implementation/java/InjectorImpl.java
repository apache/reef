/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.tang.implementation.java;

import org.apache.reef.tang.*;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.exceptions.*;
import org.apache.reef.tang.implementation.*;
import org.apache.reef.tang.types.*;
import org.apache.reef.tang.util.MonotonicHashSet;
import org.apache.reef.tang.util.MonotonicSet;
import org.apache.reef.tang.util.ReflectionUtilities;
import org.apache.reef.tang.util.TracingMonotonicTreeMap;

import java.lang.reflect.InvocationTargetException;
import java.util.*;

public class InjectorImpl implements Injector {
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

    @Override
    protected String toAmbiguousInjectString() {
      throw new UnsupportedOperationException();
    }

    @Override
    protected String toInfeasibleInjectString() {
      throw new UnsupportedOperationException();
    }

    @Override
    protected boolean isInfeasibleLeaf() {
      throw new UnsupportedOperationException();
    }

    @Override
    public String toShallowString() {
      throw new UnsupportedOperationException();
    }

  };
  private final Map<ClassNode<?>, Object> instances = new TracingMonotonicTreeMap<>();
  private final Map<NamedParameterNode<?>, Object> namedParameterInstances = new TracingMonotonicTreeMap<>();
  private final Configuration c;
  private final ClassHierarchy namespace;
  private final JavaClassHierarchy javaNamespace;
  private final Set<InjectionFuture<?>> pendingFutures = new HashSet<>();
  private boolean concurrentModificationGuard = false;
  private Aspect aspect;

  public InjectorImpl(final Configuration c) throws BindException {
    this.c = c;
    this.namespace = c.getClassHierarchy();
    this.javaNamespace = (ClassHierarchyImpl) this.namespace;
  }

  private static InjectorImpl copy(final InjectorImpl old,
                                   final Configuration... configurations) throws BindException {
    final InjectorImpl i;
    try {
      final ConfigurationBuilder cb = old.c.newBuilder();
      for (final Configuration c : configurations) {
        cb.addConfiguration(c);
      }
      i = new InjectorImpl(cb.build());
    } catch (final BindException e) {
      throw new IllegalStateException(
          "Unexpected error copying configuration!", e);
    }
    for (final ClassNode<?> cn : old.instances.keySet()) {
      if (cn.getFullName().equals(ReflectionUtilities.getFullName(Injector.class))
          || cn.getFullName().equals(ReflectionUtilities.getFullName(InjectorImpl.class))) {
        // This would imply that we're treating injector as a singleton somewhere.  It should be copied fresh each time.
        throw new IllegalStateException("Injector should be copied fresh each time.");
      }
      try {
        final ClassNode<?> newCn = (ClassNode<?>) i.namespace.getNode(cn
            .getFullName());
        i.instances.put(newCn, old.instances.get(cn));
      } catch (final BindException e) {
        throw new IllegalStateException("Could not resolve name "
            + cn.getFullName() + " when copying injector", e);
      }
    }
    // Copy references to the remaining (which must have been set with
    // bindVolatileParameter())
    for (final NamedParameterNode<?> np : old.namedParameterInstances.keySet()) {
      // if (!builder.namedParameters.containsKey(np)) {
      final Object o = old.namedParameterInstances.get(np);
      final NamedParameterNode<?> newNp = (NamedParameterNode<?>) i.namespace
          .getNode(np.getFullName());
      i.namedParameterInstances.put(newNp, o);
    }
    // Fork the aspect (if any)
    if (old.aspect != null) {
      i.bindAspect(old.aspect.createChildAspect());
    }
    return i;
  }

  private void assertNotConcurrent() {
    if (concurrentModificationGuard) {
      throw new ConcurrentModificationException("Detected attempt to use Injector " +
          "from within an injected constructor!");
    }
  }

  @SuppressWarnings("unchecked")
  private <T> T getCachedInstance(final ClassNode<T> cn) {
    if (cn.getFullName().equals("org.apache.reef.tang.Injector")) {
      return (T) this; // TODO: We should be insisting on injection futures here! .forkInjector();
    } else {
      final T t = (T) instances.get(cn);
      if (t instanceof InjectionFuture) {
        throw new IllegalStateException("Found an injection future in getCachedInstance: " + cn);
      }
      return t;
    }
  }

  /**
   * Produce a list of "interesting" constructors from a set of ClassNodes.
   * <p>
   * Tang Constructors expose a isMoreSpecificThan function that embeds all
   * a skyline query over the lattices.  Precisely:
   * <p>
   * Let candidateConstructors be the union of all constructors defined by
   * ClassNodes in candidateImplementations.
   * <p>
   * This function returns a set called filteredImplementations, defined as
   * follows:
   * <p>
   * For each member f of filteredConstructors, there does not exist
   * a g in candidateConstructors s.t. g.isMoreSpecificThan(f).
   */
  private <T> List<InjectionPlan<T>> filterCandidateConstructors(
      final List<ClassNode<T>> candidateImplementations,
      final Map<Node, InjectionPlan<?>> memo) {

    final List<InjectionPlan<T>> subIps = new ArrayList<>();
    for (final ClassNode<T> thisCN : candidateImplementations) {
      final List<Constructor<T>> constructors = new ArrayList<>();
      final List<ConstructorDef<T>> constructorList = new ArrayList<>();
      if (null != c.getLegacyConstructor(thisCN)) {
        constructorList.add(c.getLegacyConstructor(thisCN));
      }
      constructorList
          .addAll(Arrays.asList(thisCN.getInjectableConstructors()));

      for (final ConstructorDef<T> def : constructorList) {
        final List<InjectionPlan<?>> args = new ArrayList<>();
        final ConstructorArg[] defArgs = def.getArgs();

        for (final ConstructorArg arg : defArgs) {
          if (!arg.isInjectionFuture()) {
            try {
              final Node argNode = namespace.getNode(arg.getName());
              buildInjectionPlan(argNode, memo);
              args.add(memo.get(argNode));
            } catch (final NameResolutionException e) {
              throw new IllegalStateException("Detected unresolvable "
                  + "constructor arg while building injection plan.  "
                  + "This should have been caught earlier!", e);
            }
          } else {
            try {
              args.add(new InjectionFuturePlan<>(namespace.getNode(arg.getName())));
            } catch (final NameResolutionException e) {
              throw new IllegalStateException("Detected unresolvable "
                  + "constructor arg while building injection plan.  "
                  + "This should have been caught earlier!", e);
            }
          }
        }
        final Constructor<T> constructor = new Constructor<>(thisCN, def,
            args.toArray(new InjectionPlan[0]));
        constructors.add(constructor);
      }
      // The constructors are embedded in a lattice defined by
      // isMoreSpecificThan().  We want to see if, amongst the injectable
      // plans, there is a unique dominant plan, and select it.

      // First, compute the set of injectable plans.
      final List<Integer> liveIndices = new ArrayList<>();
      for (int i = 0; i < constructors.size(); i++) {
        if (constructors.get(i).getNumAlternatives() > 0) {
          liveIndices.add(i);
        }
      }
      // Now, do an all-by-all comparison, removing indices that are dominated
      // by others.
      for (int i = 0; i < liveIndices.size(); i++) {
        for (int j = i + 1; j < liveIndices.size(); j++) {
          final ConstructorDef<T> ci = constructors.get(liveIndices.get(i)).getConstructorDef();
          final ConstructorDef<T> cj = constructors.get(liveIndices.get(j)).getConstructorDef();

          if (ci.isMoreSpecificThan(cj)) {
            liveIndices.remove(j);
            j--;
          } else if (cj.isMoreSpecificThan(ci)) {
            liveIndices.remove(i);
            // Done with this inner loop invocation. Check the new ci.
            i--;
            break;
          }
        }
      }
      if (constructors.size() > 0) {
        subIps.add(wrapInjectionPlans(thisCN, constructors, false,
                liveIndices.size() == 1 ? liveIndices.get(0) : -1));
      }
    }
    return subIps;
  }

  @SuppressWarnings("unchecked")
  private <T> InjectionPlan<T> buildClassNodeInjectionPlan(final ClassNode<T> cn,
                                                           final T cachedInstance,
                                                           final ClassNode<ExternalConstructor<T>> externalConstructor,
                                                           final ClassNode<T> boundImpl,
                                                           final ClassNode<T> defaultImpl,
                                                           final Map<Node, InjectionPlan<?>> memo) {

    if (cachedInstance != null) {
      return new JavaInstance<T>(cn, cachedInstance);
    } else if (externalConstructor != null) {
      buildInjectionPlan(externalConstructor, memo);
      return new Subplan<>(cn, 0, (InjectionPlan<T>) memo.get(externalConstructor));
    } else if (boundImpl != null && !cn.equals(boundImpl)) {
      // We need to delegate to boundImpl, so recurse.
      buildInjectionPlan(boundImpl, memo);
      return new Subplan<>(cn, 0, (InjectionPlan<T>) memo.get(boundImpl));
    } else if (defaultImpl != null && !cn.equals(defaultImpl)) {
      buildInjectionPlan(defaultImpl, memo);
      return new Subplan<>(cn, 0, (InjectionPlan<T>) memo.get(defaultImpl));
    } else {
      // if we're here and there isn't a bound impl or a default impl,
      // then we're bound / defaulted to ourselves, so don't add
      // other impls to the list of things to consider.
      final List<ClassNode<T>> candidateImplementations = new ArrayList<>();
      candidateImplementations.add(cn);
      final List<InjectionPlan<T>> subIps = filterCandidateConstructors(candidateImplementations, memo);
      if (subIps.size() == 1) {
        return wrapInjectionPlans(cn, subIps, false, -1);
      } else {
        return wrapInjectionPlans(cn, subIps, true, -1);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private <T> InjectionPlan<T> wrapInjectionPlans(final ClassNode<T> infeasibleNode,
                                                  final List<? extends InjectionPlan<T>> list,
                                                  final boolean forceAmbiguous,
                                                  final int selectedIndex) {
    if (list.size() == 0) {
      return new Subplan<>(infeasibleNode);
    } else if (!forceAmbiguous && list.size() == 1) {
      return list.get(0);
    } else {
      return new Subplan<>(infeasibleNode, selectedIndex, list.toArray(new InjectionPlan[0]));
    }
  }

  /**
   * Parse the bound value of np.  When possible, this returns a cached instance.
   *
   * @return null if np has not been bound.
   * @throws ParseException
   */
  @SuppressWarnings("unchecked")
  private <T> T parseBoundNamedParameter(final NamedParameterNode<T> np) {
    final T ret;

    @SuppressWarnings("rawtypes")
    final Set<Object> boundSet = c.getBoundSet((NamedParameterNode) np);
    if (!boundSet.isEmpty()) {
      final Set<T> ret2 = new MonotonicSet<>();
      for (final Object o : boundSet) {
        if (o instanceof String) {
          try {
            ret2.add(javaNamespace.parse(np, (String) o));
          } catch (final ParseException e) {
            // Parsability is now pre-checked in bindSet, so it should not be reached!
            throw new IllegalStateException("Could not parse " + o + " which was passed into " + np +
                " FIXME: Parsability is not currently checked by bindSetEntry(Node,String)", e);
          }
        } else if (o instanceof Node) {
          ret2.add((T) o);
        } else {
          throw new IllegalStateException("Unexpected object " + o + " in bound set.  " +
              "Should consist of nodes and strings");
        }
      }
      return (T) ret2;
    }
    final List<Object> boundList = c.getBoundList((NamedParameterNode) np);
    if (boundList != null) {
      final List<T> ret2 = new ArrayList<>();
      for (final Object o : boundList) {
        if (o instanceof String) {
          try {
            ret2.add(javaNamespace.parse(np, (String) o));
          } catch (final ParseException e) {
            // Parsability is now pre-checked in bindList, so it should not be reached!
            throw new IllegalStateException("Could not parse " + o + " which was passed into " + np + " FIXME: " +
                "Parsability is not currently checked by bindList(Node,List)", e);
          }
        } else if (o instanceof Node) {
          ret2.add((T) o);
        } else {
          throw new IllegalStateException("Unexpected object " + o + " in bound list.  Should consist of nodes and " +
              "strings");
        }
      }
      return (T) ret2;
    } else if (namedParameterInstances.containsKey(np)) {
      ret = (T) namedParameterInstances.get(np);
    } else {
      final String value = c.getNamedParameter(np);
      if (value == null) {
        ret = null;
      } else {
        try {
          ret = javaNamespace.parse(np, value);
          namedParameterInstances.put(np, ret);
        } catch (final BindException e) {
          throw new IllegalStateException(
              "Could not parse pre-validated value", e);
        }
      }
    }
    return ret;
  }

  @SuppressWarnings("unchecked")
  private <T> ClassNode<T> parseDefaultImplementation(final ClassNode<T> cn) {
    if (cn.getDefaultImplementation() != null) {
      try {
        return (ClassNode<T>) javaNamespace.getNode(cn.getDefaultImplementation());
      } catch (ClassCastException | NameResolutionException e) {
        throw new IllegalStateException("After validation, " + cn + " had a bad default implementation named " +
            cn.getDefaultImplementation(), e);
      }
    } else {
      return null;
    }
  }

  @SuppressWarnings({"unchecked"})
  private <T> void buildInjectionPlan(final Node n,
                                      final Map<Node, InjectionPlan<?>> memo) {
    if (memo.containsKey(n)) {
      if (BUILDING == memo.get(n)) {
        final StringBuilder loopyList = new StringBuilder("[");
        for (final Map.Entry<Node, InjectionPlan<?>> node : memo.entrySet()) {
          if (node.getValue() == BUILDING) {
            loopyList.append(" ").append(node.getKey().getFullName());
          }
        }
        loopyList.append(" ]");
        throw new ClassHierarchyException("Detected loopy constructor involving "
            + loopyList.toString());
      } else {
        return;
      }
    }
    memo.put(n, BUILDING);
    final InjectionPlan<T> ip;
    if (n instanceof NamedParameterNode) {
      final NamedParameterNode<T> np = (NamedParameterNode<T>) n;

      final T boundInstance = parseBoundNamedParameter(np);
      final T defaultInstance = javaNamespace.parseDefaultValue(np);
      final T instance = boundInstance != null ? boundInstance : defaultInstance;

      if (instance instanceof Node) {
        buildInjectionPlan((Node) instance, memo);
        ip = new Subplan<T>(n, 0, (InjectionPlan<T>) memo.get(instance));
      } else if (instance instanceof Set) {
        final Set<T> entries = (Set<T>) instance;
        final Set<InjectionPlan<T>> plans = new MonotonicHashSet<>();
        for (final T entry : entries) {
          if (entry instanceof ClassNode) {
            buildInjectionPlan((ClassNode<?>) entry, memo);
            plans.add((InjectionPlan<T>) memo.get(entry));
          } else {
            plans.add(new JavaInstance<T>(n, entry));
          }

        }
        ip = new SetInjectionPlan<T>(n, plans);
      } else if (instance instanceof List) {
        final List<T> entries = (List<T>) instance;
        final List<InjectionPlan<T>> plans = new ArrayList<>();
        for (final T entry : entries) {
          if (entry instanceof ClassNode) {
            buildInjectionPlan((ClassNode<?>) entry, memo);
            plans.add((InjectionPlan<T>) memo.get(entry));
          } else {
            plans.add(new JavaInstance<T>(n, entry));
          }
        }
        ip = new ListInjectionPlan<T>(n, plans);
      } else {
        ip = new JavaInstance<T>(np, instance);
      }
    } else if (n instanceof ClassNode) {
      final ClassNode<T> cn = (ClassNode<T>) n;

      // Any (or all) of the next four values might be null; that's fine.
      final T cached = getCachedInstance(cn);
      final ClassNode<T> boundImpl = c.getBoundImplementation(cn);
      final ClassNode<T> defaultImpl = parseDefaultImplementation(cn);
      final ClassNode<ExternalConstructor<T>> ec = c.getBoundConstructor(cn);

      ip = buildClassNodeInjectionPlan(cn, cached, ec, boundImpl, defaultImpl, memo);
    } else if (n instanceof PackageNode) {
      throw new IllegalArgumentException(
          "Request to instantiate Java package as object");
    } else {
      throw new IllegalStateException(
          "Type hierarchy contained unknown node type!:" + n);
    }
    memo.put(n, ip);
  }

  /**
   * Return an injection plan for the given class / parameter name.
   *
   * @param n The name of an injectable class or interface, or a NamedParameter.
   * @return
   * @throws NameResolutionException
   */
  public InjectionPlan<?> getInjectionPlan(final Node n) {
    final Map<Node, InjectionPlan<?>> memo = new HashMap<>();
    buildInjectionPlan(n, memo);
    return memo.get(n);
  }

  @Override
  public InjectionPlan<?> getInjectionPlan(final String name) throws NameResolutionException {
    return getInjectionPlan(namespace.getNode(name));
  }

  @SuppressWarnings("unchecked")
  public <T> InjectionPlan<T> getInjectionPlan(final Class<T> name) {
    return (InjectionPlan<T>) getInjectionPlan(javaNamespace.getNode(name));
  }

  @Override
  public boolean isInjectable(final String name) throws NameResolutionException {
    return getInjectionPlan(namespace.getNode(name)).isInjectable();
  }

  @Override
  public boolean isInjectable(final Class<?> clazz) {
    try {
      return isInjectable(ReflectionUtilities.getFullName(clazz));
    } catch (final NameResolutionException e) {
      throw new IllegalStateException("Could not round trip " + clazz + " through ClassHierarchy", e);
    }
  }

  @Override
  public boolean isParameterSet(final String name) throws NameResolutionException {
    final InjectionPlan<?> p = getInjectionPlan(namespace.getNode(name));
    return p.isInjectable();
  }

  @Override
  public boolean isParameterSet(final Class<? extends Name<?>> name)
      throws BindException {
    return isParameterSet(name.getName());
  }

  private <U> U getInstance(final Node n) throws InjectionException {
    assertNotConcurrent();
    @SuppressWarnings("unchecked") final InjectionPlan<U> plan = (InjectionPlan<U>) getInjectionPlan(n);
    final U u = (U) injectFromPlan(plan);

    while (!pendingFutures.isEmpty()) {
      final Iterator<InjectionFuture<?>> i = pendingFutures.iterator();
      final InjectionFuture<?> f = i.next();
      pendingFutures.remove(f);
      f.get();
    }
    return u;
  }

  @Override
  public <U> U getInstance(final Class<U> clazz) throws InjectionException {
    if (Name.class.isAssignableFrom(clazz)) {
      throw new InjectionException("getInstance() called on Name "
          + ReflectionUtilities.getFullName(clazz)
          + " Did you mean to call getNamedInstance() instead?");
    }
    return getInstance(javaNamespace.getNode(clazz));
  }

  @SuppressWarnings("unchecked")
  @Override
  public <U> U getInstance(final String clazz) throws InjectionException, NameResolutionException {
    return (U) getInstance(namespace.getNode(clazz));
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T getNamedInstance(final Class<? extends Name<T>> clazz)
      throws InjectionException {
    return (T) getInstance(javaNamespace.getNode(clazz));
  }

  public <T> T getNamedParameter(final Class<? extends Name<T>> clazz)
      throws InjectionException {
    return getNamedInstance(clazz);
  }

  private <T> java.lang.reflect.Constructor<T> getConstructor(
      final ConstructorDef<T> constructor) throws ClassNotFoundException,
      NoSuchMethodException, SecurityException {
    @SuppressWarnings("unchecked") final Class<T> clazz =
        (Class<T>) javaNamespace.classForName(constructor.getClassName());
    final ConstructorArg[] args = constructor.getArgs();
    final Class<?>[] parameterTypes = new Class[args.length];
    for (int i = 0; i < args.length; i++) {
      if (args[i].isInjectionFuture()) {
        parameterTypes[i] = InjectionFuture.class;
      } else {
        parameterTypes[i] = javaNamespace.classForName(args[i].getType());
      }
    }
    final java.lang.reflect.Constructor<T> cons = clazz
        .getDeclaredConstructor(parameterTypes);
    cons.setAccessible(true);
    return cons;
  }

  /**
   * This gets really nasty now that constructors can invoke operations on us.
   * The upshot is that we should check to see if instances have been
   * registered by callees after each recursive invocation of injectFromPlan or
   * constructor invocations. The error handling currently bails if the thing we
   * just instantiated should be discarded.
   * <p>
   * This could happen if (for instance), a constructor did a
   * bindVolatileInstance of its own class to an instance, or somehow triggered
   * an injection of itself with a different plan (an injection of itself with
   * the same plan would lead to an infinite recursion, so it's not really our
   * problem).
   *
   * @param plan
   * @return
   * @throws InjectionException
   */
  @SuppressWarnings("unchecked")
  private <T> T injectFromPlan(final InjectionPlan<T> plan) throws InjectionException {

    if (!plan.isFeasible()) {
      throw new InjectionException("Cannot inject " + plan.getNode().getFullName() + ": "
          + plan.toCantInjectString());
    }
    if (plan.isAmbiguous()) {
      throw new InjectionException("Cannot inject " + plan.getNode().getFullName() + " "
          + plan.toCantInjectString());
    }
    if (plan instanceof InjectionFuturePlan) {
      final InjectionFuturePlan<T> fut = (InjectionFuturePlan<T>) plan;
      final String key = fut.getNode().getFullName();
      try {
        final InjectionFuture<?> ret = new InjectionFuture<>(
            this, javaNamespace.classForName(fut.getNode().getFullName()));
        pendingFutures.add(ret);
        return (T) ret;
      } catch (final ClassNotFoundException e) {
        throw new InjectionException("Could not get class for " + key, e);
      }
    } else if (plan.getNode() instanceof ClassNode && null != getCachedInstance((ClassNode<T>) plan.getNode())) {
      return getCachedInstance((ClassNode<T>) plan.getNode());
    } else if (plan instanceof JavaInstance) {
      // TODO: Must be named parameter node.  Check.
//      throw new IllegalStateException("Instance from plan not in Injector's set of instances?!?");
      return ((JavaInstance<T>) plan).getInstance();
    } else if (plan instanceof Constructor) {
      final Constructor<T> constructor = (Constructor<T>) plan;
      final Object[] args = new Object[constructor.getArgs().length];
      final InjectionPlan<?>[] argPlans = constructor.getArgs();

      for (int i = 0; i < argPlans.length; i++) {
        args[i] = injectFromPlan(argPlans[i]);
      }
      try {
        concurrentModificationGuard = true;
        T ret;
        try {
          final ConstructorDef<T> def = constructor.getConstructorDef();
          final java.lang.reflect.Constructor<T> construct = getConstructor(def);

          if (aspect != null) {
            ret = aspect.inject(def, construct, args);
          } else {
            ret = construct.newInstance(args);
          }
        } catch (final IllegalArgumentException e) {
          final StringBuilder sb = new StringBuilder("Internal Tang error?  Could not call constructor " +
              constructor.getConstructorDef() + " with arguments [");
          for (final Object o : args) {
            sb.append("\n\t" + o);
          }
          sb.append("]");
          throw new IllegalStateException(sb.toString(), e);
        }
        if (ret instanceof ExternalConstructor) {
          ret = ((ExternalConstructor<T>) ret).newInstance();
        }
        instances.put(constructor.getNode(), ret);
        return ret;
      } catch (final ReflectiveOperationException e) {
        throw new InjectionException("Could not invoke constructor: " + plan,
            e instanceof InvocationTargetException ? e.getCause() : e);
      } finally {
        concurrentModificationGuard = false;
      }
    } else if (plan instanceof Subplan) {
      final Subplan<T> ambiguous = (Subplan<T>) plan;
      return injectFromPlan(ambiguous.getDelegatedPlan());
    } else if (plan instanceof SetInjectionPlan) {
      final SetInjectionPlan<T> setPlan = (SetInjectionPlan<T>) plan;
      final Set<T> ret = new MonotonicHashSet<>();
      for (final InjectionPlan<T> subplan : setPlan.getEntryPlans()) {
        ret.add(injectFromPlan(subplan));
      }
      return (T) ret;
    } else if (plan instanceof ListInjectionPlan) {
      final ListInjectionPlan<T> listPlan = (ListInjectionPlan<T>) plan;
      final List<T> ret = new ArrayList<>();
      for (final InjectionPlan<T> subplan : listPlan.getEntryPlans()) {
        ret.add(injectFromPlan(subplan));
      }
      return (T) ret;
    } else {
      throw new IllegalStateException("Unknown plan type: " + plan);
    }
  }

  @Override
  public <T> void bindVolatileInstance(final Class<T> cl, final T o) throws BindException {
    bindVolatileInstanceNoCopy(cl, o);
  }

  @Override
  public <T> void bindVolatileParameter(final Class<? extends Name<T>> cl, final T o)
      throws BindException {
    bindVolatileParameterNoCopy(cl, o);
  }

  <T> void bindVolatileInstanceNoCopy(final Class<T> cl, final T o) throws BindException {
    assertNotConcurrent();
    final Node n = javaNamespace.getNode(cl);
    if (n instanceof ClassNode) {
      final ClassNode<?> cn = (ClassNode<?>) n;
      final Object old = getCachedInstance(cn);
      if (old != null) {
        throw new BindException("Attempt to re-bind instance.  Old value was "
            + old + " new value is " + o);
      }
      instances.put(cn, o);
    } else {
      throw new IllegalArgumentException("Expected Class but got " + cl
          + " (probably a named parameter).");
    }
  }

  <T> void bindVolatileParameterNoCopy(final Class<? extends Name<T>> cl, final T o)
      throws BindException {
    final Node n = javaNamespace.getNode(cl);
    if (n instanceof NamedParameterNode) {
      final NamedParameterNode<?> np = (NamedParameterNode<?>) n;
      final Object old = this.c.getNamedParameter(np);
      if (old != null) {
        // XXX need to get the binding site here!
        throw new BindException(
            "Attempt to re-bind named parameter " + ReflectionUtilities.getFullName(cl) + ".  Old value was [" + old
                + "] new value is [" + o + "]");
      }
      try {
        namedParameterInstances.put(np, o);
      } catch (final IllegalArgumentException e) {
        throw new BindException(
            "Attempt to bind named parameter " + ReflectionUtilities.getFullName(cl) + " failed. "
                + "Value is [" + o + "]", e);

      }
    } else {
      throw new IllegalArgumentException("Expected Name, got " + cl
          + " (probably a class)");
    }
  }

  @Override
  public Injector forkInjector() {
    try {
      return forkInjector(new Configuration[0]);
    } catch (final BindException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public Injector forkInjector(final Configuration... configurations)
      throws BindException {
    final InjectorImpl ret;
    ret = copy(this, configurations);
    return ret;
  }

  @Override
  public <T> void bindAspect(final Aspect a) throws BindException {
    if (aspect != null) {
      throw new BindException("Attempt to re-bind aspect! old=" + aspect + " new=" + a);
    }
    aspect = a;
  }

  @Override
  public Aspect getAspect() {
    return aspect;
  }
}
