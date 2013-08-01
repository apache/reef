package com.microsoft.tang.implementation.java;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import com.microsoft.tang.ClassHierarchy;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.ConfigurationBuilder;
import com.microsoft.tang.ExternalConstructor;
import com.microsoft.tang.InjectionFuture;
import com.microsoft.tang.Injector;
import com.microsoft.tang.JavaClassHierarchy;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.exceptions.NameResolutionException;
import com.microsoft.tang.implementation.Constructor;
import com.microsoft.tang.implementation.InjectionFuturePlan;
import com.microsoft.tang.implementation.InjectionPlan;
import com.microsoft.tang.implementation.Subplan;
import com.microsoft.tang.types.ClassNode;
import com.microsoft.tang.types.ConstructorArg;
import com.microsoft.tang.types.ConstructorDef;
import com.microsoft.tang.types.NamedParameterNode;
import com.microsoft.tang.types.Node;
import com.microsoft.tang.types.PackageNode;
import com.microsoft.tang.util.ReflectionUtilities;
import com.microsoft.tang.util.TracingMonotonicMap;

public class InjectorImpl implements Injector {
  final Map<ClassNode<?>, Object> instances = new TracingMonotonicMap<>();
  final Map<NamedParameterNode<?>, Object> namedParameterInstances = new TracingMonotonicMap<>();

  private boolean concurrentModificationGuard = false;
  private static final AtomicBoolean warned = new AtomicBoolean(false);
  private void assertNotConcurrent() {
    if(concurrentModificationGuard) {
      //throw new ConcurrentModificationException("Detected attempt to modify Injector from within an injected constructor!");
      if(warned.compareAndSet(false, true)) {
        System.err.println("Tang warning: Detected modification of injector state from within injected constructor.  This warning will eventually become an error.");
      }
    }
  }
  
  private final Configuration c;
  private final ClassHierarchy namespace;
  private final JavaClassHierarchy javaNamespace;
  private final Set<InjectionFuture<?>> pendingFutures = new HashSet<>();
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

    @Override
    public boolean hasFutureDependency() {
      throw new UnsupportedOperationException();
    }

  };

  @SuppressWarnings("unchecked")
  private <T> T getCachedInstance(ClassNode<T> cn) {
    if(cn.getFullName().equals("com.microsoft.tang.Injector")) {
      return (T)this.forkInjector();
    } else {
      return (T)instances.get(cn);
    }
  }
  
  @SuppressWarnings("unchecked")
  private InjectionPlan<?> wrapInjectionPlans(Node infeasibleNode,
      List<? extends InjectionPlan<?>> list, boolean forceAmbiguous, int selectedIndex) {
    if (list.size() == 0) {
      return new Subplan<>(infeasibleNode);
    } else if ((!forceAmbiguous) && list.size() == 1) {
      return list.get(0);
    } else {
      return new Subplan<>(infeasibleNode, selectedIndex, list.toArray(new InjectionPlan[0]));
    }
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private void buildInjectionPlan(final Node n,
      Map<Node, InjectionPlan<?>> memo) {
    if (memo.containsKey(n)) {
      if (BUILDING == memo.get(n)) {
        throw new IllegalStateException("Detected loopy constructor involving "
            + n.getFullName());
      } else {
        return;
      }
    }
    memo.put(n, BUILDING);
    final InjectionPlan<?> ip;
    if (n instanceof NamedParameterNode) {
      NamedParameterNode<?> np = (NamedParameterNode<?>) n;
      Object instance = namedParameterInstances.get(n);
      if (instance == null) {
        String value = c.getNamedParameter(np);
        try {
          if (value != null) {
            instance = javaNamespace.parse(np, value);
            namedParameterInstances.put(np, instance);
          } else {
            instance = javaNamespace.parseDefaultValue(np);
          }
        } catch (BindException e) {
          throw new IllegalStateException(
              "Could not parse pre-validated value", e);
        }
      }
      if (instance instanceof ClassNode) {
        ClassNode<?> instanceCN = (ClassNode<?>)instance;
        buildInjectionPlan(instanceCN, memo);
        ip = new Subplan<>(np, 0, memo.get(instanceCN));
      } else {
        ip = new JavaInstance<Object>(np, instance);
      }
    } else if (n instanceof ClassNode) {
      final ClassNode<?> cn = (ClassNode<?>) n;
      final ClassNode<?> boundImpl = c.getBoundImplementation(cn);
      final ClassNode<?> defaultImpl;
      if(cn.getDefaultImplementation() != null) {
        try {
          defaultImpl = (ClassNode<?>)javaNamespace.getNode(cn.getDefaultImplementation());
        } catch(NameResolutionException | ClassCastException e) {
          throw new IllegalStateException(cn + " has a bad default implementation named " + cn.getDefaultImplementation(), e);
        }
      } else {
        defaultImpl = null;
      }
      Object cached = getCachedInstance(cn);
      if (cached != null) {
        ip = new JavaInstance<Object>(cn, cached);
      } else if (null != c.getBoundConstructor(cn)) {
        ClassNode<? extends ExternalConstructor> ec = c.getBoundConstructor(cn);
        buildInjectionPlan(ec, memo);
        ip = new Subplan(cn, 0, memo.get(ec));
        memo.put(cn, ip);
      } else if (boundImpl != null && !cn.equals(boundImpl)) {
        // We need to delegate to boundImpl, so recurse.
        buildInjectionPlan(boundImpl, memo);
        ip = new Subplan(cn, 0, memo.get(boundImpl));
        memo.put(cn, ip);
      } else if (defaultImpl != null && !cn.equals(defaultImpl)) {
        buildInjectionPlan(defaultImpl, memo);
        ip = new Subplan(cn, 0, memo.get(defaultImpl));
        memo.put(cn, ip);
      } else {
        List<ClassNode<?>> classNodes = new ArrayList<>();
        // if we're here and there is a bound impl or a default impl,
        // then we're bound / defaulted to ourselves, so don't add
        // other impls to the list of things to consider.
        
        if (boundImpl == null && defaultImpl == null) {
          classNodes.addAll(cn.getKnownImplementations());
        }
        classNodes.add(cn);
        List<InjectionPlan<?>> sub_ips = new ArrayList<InjectionPlan<?>>();
        for (ClassNode<?> thisCN : classNodes) {
          final List<Constructor<?>> constructors = new ArrayList<>();
          final List<ConstructorDef<?>> constructorList = new ArrayList<>();
          if (null != c.getLegacyConstructor(thisCN)) {
            constructorList.add(c.getLegacyConstructor(thisCN));
          }
          constructorList.addAll(Arrays.asList(thisCN
              .getInjectableConstructors()));

          for (ConstructorDef<?> def : constructorList) {
            List<InjectionPlan<?>> args = new ArrayList<InjectionPlan<?>>();
            ConstructorArg[] defArgs = def.getArgs();

            for (ConstructorArg arg : defArgs) {
              if(!arg.isInjectionFuture()) {
                try {
                  Node argNode = namespace.getNode(arg.getName());
                  buildInjectionPlan(argNode, memo);
                  args.add(memo.get(argNode));
                } catch (NameResolutionException e) {
                  throw new IllegalStateException("Detected unresolvable "
                      + "constructor arg while building injection plan.  "
                      + "This should have been caught earlier!", e);
                }
              } else {
                try {
                  args.add(new InjectionFuturePlan(namespace.getNode(arg.getName())));
                } catch (NameResolutionException e) {
                  throw new IllegalStateException("Detected unresolvable "
                      + "constructor arg while building injection plan.  "
                      + "This should have been caught earlier!", e);
                }
              }
            }
            Constructor constructor = new Constructor(thisCN, def,
                args.toArray(new InjectionPlan[0]));
            constructors.add(constructor);
          }
          // The constructors are embedded in a lattice defined by isMoreSpecificThan().
          // We want to see if, amongst the injectable plans, there is a unique dominant
          // plan, and select it.
          
          // First, compute the set of injectable plans.
          List<Integer> liveIndices = new ArrayList<>();
          for(int i = 0; i < constructors.size(); i++) {
            if(constructors.get(i).getNumAlternatives() > 0) {
              liveIndices.add(i);
            }
          }
          // Now, do an all-by-all comparison, removing indices that are dominated by others.
          for(int i = 0; i < liveIndices.size(); i++) {
            for(int j = i+1; j < liveIndices.size(); j++) {
              ConstructorDef ci = constructors.get(liveIndices.get(i)).getConstructorDef();
              ConstructorDef cj = constructors.get(liveIndices.get(j)).getConstructorDef();
              
              if(ci.isMoreSpecificThan(cj)) {
                liveIndices.remove(j);
                j--;
              } else if(cj.isMoreSpecificThan(ci)) {
                liveIndices.remove(i);
                // Done with this inner loop invocation.  Check the new ci.
                i--;
                break;
              }
            }
          }
          sub_ips.add(wrapInjectionPlans(thisCN, constructors, false, liveIndices.size() == 1 ? liveIndices.get(0) : -1));
        }
        if (classNodes.size() == 1
            && classNodes.get(0).getFullName().equals(n.getFullName())) {
          ip = wrapInjectionPlans(n, sub_ips, false, -1);
        } else {
          ip = wrapInjectionPlans(n, sub_ips, true, -1);
        }
      }
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
   * @param name
   *          The name of an injectable class or interface, or a NamedParameter.
   * @return
   * @throws NameResolutionException
   */
  public InjectionPlan<?> getInjectionPlan(final Node n) {
    assertNotConcurrent();
    Map<Node, InjectionPlan<?>> memo = new HashMap<>();
    buildInjectionPlan(n, memo);
    return memo.get(n);
  }

  @SuppressWarnings("unchecked")
  public <T> InjectionPlan<T> getInjectionPlan(Class<T> name) {
    return (InjectionPlan<T>) getInjectionPlan(javaNamespace.getNode(name));
  }

  @Override
  public boolean isInjectable(String name) throws NameResolutionException {
    return getInjectionPlan(namespace.getNode(name)).isInjectable();
  }

  @Override
  public boolean isInjectable(Class<?> clazz) {
    assertNotConcurrent();
    try {
      return isInjectable(ReflectionUtilities.getFullName(clazz));
    } catch(NameResolutionException e) {
      throw new IllegalStateException("Could not round trip " + clazz + " through ClassHierarchy", e);
    }
  }

  @Override
  public boolean isParameterSet(String name) throws NameResolutionException {
    InjectionPlan<?> p = getInjectionPlan(namespace.getNode(name));
    return p.isInjectable();
  }

  @Override
  public boolean isParameterSet(Class<? extends Name<?>> name)
      throws BindException {
    return isParameterSet(name.getName());
  }

  public InjectorImpl(Configuration c) throws BindException {
    this.c = c;
    this.namespace = c.getClassHierarchy();
    this.javaNamespace = (ClassHierarchyImpl) this.namespace;
  }

  private <U> U getInstance(Node n) throws InjectionException {
    assertNotConcurrent();
    @SuppressWarnings("unchecked")
    InjectionPlan<U> plan = (InjectionPlan<U>)getInjectionPlan(n);
    U u = (U) injectFromPlan(plan);
    
    while(!pendingFutures.isEmpty()) {
      Iterator<InjectionFuture<?>> i = pendingFutures.iterator();
      while(i.hasNext()) {
        InjectionFuture<?> f = i.next();
        pendingFutures.remove(f);
        f.get();
      }
    }
    return u;
  }
  @Override
  public <U> U getInstance(Class<U> clazz) throws InjectionException {
    if (Name.class.isAssignableFrom(clazz)) {
      throw new InjectionException("getInstance() called on Name "
          + ReflectionUtilities.getFullName(clazz)
          + " Did you mean to call getNamedInstance() instead?");
    }
    return getInstance(javaNamespace.getNode(clazz));
  }

  @SuppressWarnings("unchecked")
  @Override
  public <U> U getInstance(String clazz) throws InjectionException, NameResolutionException {
    return (U) getInstance(namespace.getNode(clazz));
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T getNamedInstance(Class<? extends Name<T>> clazz)
      throws InjectionException {
    return (T) getInstance(javaNamespace.getNode(clazz));
  }
  public <T> T getNamedParameter(Class<? extends Name<T>> clazz)
      throws InjectionException {
    return getNamedInstance(clazz);
  }

  private <T> java.lang.reflect.Constructor<T> getConstructor(
      ConstructorDef<T> constructor) throws ClassNotFoundException,
      NoSuchMethodException, SecurityException {
    @SuppressWarnings("unchecked")
    Class<T> clazz = (Class<T>) javaNamespace.classForName(constructor
        .getClassName());
    ConstructorArg[] args = constructor.getArgs();
    Class<?> parameterTypes[] = new Class[args.length];
    for (int i = 0; i < args.length; i++) {
      if(args[i].isInjectionFuture()) {
        parameterTypes[i] = InjectionFuture.class;
      } else {
        parameterTypes[i] = javaNamespace.classForName(args[i].getType());
      }
    }
    java.lang.reflect.Constructor<T> cons = clazz
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
   * 
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
  private <T> T injectFromPlan(InjectionPlan<T> plan) throws InjectionException {

    if (!plan.isFeasible()) {
      throw new InjectionException("Cannot inject " + plan.getNode().getFullName() + ": "
          + plan.toCantInjectString());
    }
    if (plan.isAmbiguous()) {
      throw new InjectionException("Cannot inject " + plan.getNode().getFullName() + " "
          + plan.toCantInjectString());
    }
    if(plan.getNode() instanceof ClassNode) {
      T cached = getCachedInstance((ClassNode<T>)plan.getNode());
      if (cached != null) {
        return cached;
      }
    }
    if (plan instanceof InjectionFuturePlan) {
      InjectionFuturePlan<T> fut = (InjectionFuturePlan<T>)plan;
      final String key = fut.getNode().getFullName();
      try {
        InjectionFuture<?> ret = new InjectionFuture<>(this, javaNamespace.classForName(fut.getNode().getFullName()));
        pendingFutures.add(ret);
        return (T)ret;
      } catch(ClassNotFoundException e) {
        throw new InjectionException("Could not get class for " + key);
      }
    } else if (plan instanceof JavaInstance) {
      // TODO: Must be named parameter node.  Check.
//      throw new IllegalStateException("Instance from plan not in Injector's set of instances?!?");
      return ((JavaInstance<T>) plan).instance;
    } else if (plan instanceof Constructor) {
      final Constructor<T> constructor = (Constructor<T>) plan;
      final Object[] args = new Object[constructor.getArgs().length];
      final InjectionPlan<?>[] argPlans = constructor.getArgs();

      for (int i = 0; i < argPlans.length; i++) {
        args[i] = injectFromPlan(argPlans[i]);
      }
      try {
        T ret;
        concurrentModificationGuard = true;
        ret = getConstructor(
            (ConstructorDef<T>) constructor.getConstructorDef()).newInstance(
            args);
        
        if (ret instanceof ExternalConstructor) {
      	  ret = ((ExternalConstructor<T>)ret).newInstance();
        }
        concurrentModificationGuard = false;
        instances.put(constructor.getNode(), ret);
        return ret;
      } catch (ReflectiveOperationException e) {
        throw new InjectionException("Could not invoke constructor", e);
      }
    } else if (plan instanceof Subplan) {
      Subplan<T> ambiguous = (Subplan<T>) plan;
      if (ambiguous.isInjectable()) {
        Object ret = injectFromPlan(ambiguous.getDelegatedPlan());
        // TODO: Check "T" in "instanceof ExternalConstructor<T>"
        if (ret instanceof ExternalConstructor) {
          // TODO fix up generic types for injectFromPlan with external
          // constructor!
          concurrentModificationGuard = true;
          T val = ((ExternalConstructor<T>) ret).newInstance();
          concurrentModificationGuard = false;
          // XXX Looks like a bug.  What if caller asked for an instance of the external constructor
          instances.put((ClassNode<?>)plan.getNode(), ret);
          return val;
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
      Configuration... configurations) throws BindException {
    final InjectorImpl i;
    try {
      final ConfigurationBuilder cb = old.c.newBuilder();
      for (Configuration c : configurations) {
        cb.addConfiguration(c);
      }
      i = new InjectorImpl(cb.build());
    } catch (BindException e) {
      throw new IllegalStateException(
          "Unexpected error copying configuration!", e);
    }
    for (ClassNode<?> cn : old.instances.keySet()) {
      if (!(cn.getFullName().equals("com.microsoft.tang.Injector"))) {
        try {
          ClassNode<?> new_cn = (ClassNode<?>) i.namespace.getNode(cn
              .getFullName());
          i.instances.put(new_cn, old.instances.get(cn));
        } catch (BindException e) {
          throw new IllegalStateException("Could not resolve name "
              + cn.getFullName() + " when copying injector");
        }
      }
    }
    // Copy references to the remaining (which must have been set with
    // bindVolatileParameter())
    for (NamedParameterNode<?> np : old.namedParameterInstances.keySet()) {
      // if (!builder.namedParameters.containsKey(np)) {
      Object o = old.namedParameterInstances.get(np);
      NamedParameterNode<?> new_np = (NamedParameterNode<?>) i.namespace
          .getNode(np.getFullName());
      i.namedParameterInstances.put(new_np, o);
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
    assertNotConcurrent();
    Node n = javaNamespace.getNode(c);
    if (n instanceof ClassNode) {
      ClassNode<?> cn = (ClassNode<?>) n;
      Object old = getCachedInstance(cn);
      if (old != null) {
        throw new BindException("Attempt to re-bind instance.  Old value was "
            + old + " new value is " + o);
      }
      instances.put(cn, o);
    } else {
      throw new IllegalArgumentException("Expected Class but got " + c
          + " (probably a named parameter).");
    }
  }

  <T> void bindVolatileParameterNoCopy(Class<? extends Name<T>> c, T o)
      throws BindException {
    Node n = javaNamespace.getNode(c);
    if (n instanceof NamedParameterNode) {
      NamedParameterNode<?> np = (NamedParameterNode<?>) n;
      Object old = this.c.getNamedParameter(np);
      if(old != null) {
        // XXX need to get the binding site here!
        throw new BindException(
            "Attempt to re-bind named parameter " + ReflectionUtilities.getFullName(c) + ".  Old value was [" + old
                + "] new value is [" + o + "]");
      }
      try {
        namedParameterInstances.put(np, o);
      } catch (IllegalArgumentException e) {
        throw new BindException(
            "Attempt to re-bind named parameter " + ReflectionUtilities.getFullName(c) + ".  Old value was [" + old
            + "] new value is [" + o + "]");

      }
    } else {
      throw new IllegalArgumentException("Expected Name, got " + c
          + " (probably a class)");
    }
  }

  @Override
  public Injector createChildInjector(Configuration... configurations)
      throws BindException {
    return forkInjector(configurations);
  }

  @Override
  public Injector forkInjector() {
    try {
      return forkInjector(new Configuration[0]);
    } catch (BindException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public Injector forkInjector(Configuration... configurations)
      throws BindException {
    assertNotConcurrent();
    InjectorImpl ret;
    ret = copy(this, configurations);
    return ret;
  }
}