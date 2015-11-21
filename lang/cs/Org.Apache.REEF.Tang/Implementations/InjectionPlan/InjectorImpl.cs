/**
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

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Types;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Tang.Implementations.InjectionPlan
{
    public class InjectorImpl : IInjector
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(InjectorImpl));

        readonly IDictionary<INamedParameterNode, object> namedParameterInstances = new MonotonicTreeMap<INamedParameterNode, object>();
        private readonly ICsClassHierarchy classHierarchy;
        private readonly IConfiguration configuration;
        readonly IDictionary<IClassNode, Object> instances = new MonotonicTreeMap<IClassNode, Object>();
        private Aspect aspect;
        private readonly ISet<IInjectionFuture<object>> pendingFutures = new HashSet<IInjectionFuture<object>>();

        static readonly InjectionPlan BUILDING = new BuildingInjectionPlan(null); // TODO anonymous class


        private bool concurrentModificationGuard = false;

        private void AssertNotConcurrent()
        {
            if (concurrentModificationGuard)
            {
                // TODO
                // throw new ConcurrentModificationException("Detected attempt to use Injector from within an injected constructor!");
            }
        }
        public InjectorImpl(IConfiguration c)
        {
            this.configuration = c;
            this.classHierarchy = (ICsClassHierarchy)c.GetClassHierarchy();
        }

        public object InjectFromPlan(InjectionPlan plan)
        {
            if (!plan.IsFeasible())
            {
                var ex = new InjectionException("Cannot inject " + plan.GetNode().GetFullName() + ": "
                    + plan.ToCantInjectString());
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
            }

            if (plan.IsAmbiguous())
            {
                var ex = new InjectionException("Cannot inject " + plan.GetNode().GetFullName() + " "
                    + plan.ToCantInjectString());
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
            }

            if (plan is InjectionFuturePlan)
            {
                InjectionFuturePlan fut = (InjectionFuturePlan)plan;
                INode node = fut.GetNode();
                string key = node.GetFullName();
                try
                {
                    Type t = null;
                    Type nodeType = classHierarchy.ClassForName(node.GetFullName());
                    
                    if (node is IClassNode)
                    {
                        t = nodeType; 
                    }
                    else if (node is INamedParameterNode)
                    {
                        var nn = (INamedParameterNode)node;
                        t = classHierarchy.ClassForName(nn.GetFullArgName());
                        if (nn.IsSet())
                        {
                            t = typeof(ISet<>).MakeGenericType(new Type[] { t });
                        }
                    }
                    else
                    {
                        var ex = new ApplicationException("Unexpected node type. Wanted ClassNode or NamedParameterNode.  Got: " + node);
                        Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
                    }
                    
                    // Java - InjectionFuture<?> ret = new InjectionFuture<>(this, javaNamespace.classForName(fut.getNode().getFullName()));
                    // C# - InjectionFuture<object> ret = new InjectionFutureImpl<object>(this, classHierarchy.ClassForName(fut.GetNode().GetFullName()));
                    // We cannot simply create an object from generic with object as <T>
                    // typeof(InjectionFutureImpl<>).MakeGenericType(t) will get the InjectionFutureImpl generic Type with <T> as t 
                    // for ClassNode, t is the Type of the class, for NamedParameterNode, t is the Type of the argument
                    // we then use reflection to invoke the constructor
                    // To retain generic argument information??                   
                    Type injectionFuture = typeof(InjectionFutureImpl<>).MakeGenericType(t);
                    var constructor = injectionFuture.GetConstructor(new Type[] { typeof(IInjector), typeof(Type) });
                    IInjectionFuture<object> ret = (IInjectionFuture<object>)constructor.Invoke(new object[] { this, nodeType }); 

                    pendingFutures.Add(ret);
                    return ret;
                }
                catch (TypeLoadException e)
                {
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, LOGGER);
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new InjectionException("Could not get class for " + key), LOGGER); 
                }
            }
            else if (plan.GetNode() is IClassNode && null != GetCachedInstance((IClassNode)plan.GetNode()))
            {
                return GetCachedInstance((IClassNode)plan.GetNode());
            }
            else if (plan is CsInstance)
            {
                // TODO: Must be named parameter node.  Check.
                // throw new IllegalStateException("Instance from plan not in Injector's set of instances?!?");
                return ((CsInstance)plan).instance;
            }
            else if (plan is Constructor)
            {
                Constructor constructor = (Constructor)plan;
                object[] args = new object[constructor.GetArgs().Length];
                InjectionPlan[] argPlans = constructor.GetArgs();

                for (int i = 0; i < argPlans.Length; i++)
                {
                    args[i] = InjectFromPlan(argPlans[i]);
                }

                try
                {
                    concurrentModificationGuard = true;
                    object ret = null;
                    try
                    {
                        IConstructorDef def = (IConstructorDef)constructor.GetConstructorDef();
                        ConstructorInfo c = GetConstructor(def);

                        if (aspect != null)
                        {
                            ret = aspect.Inject(def, c, args);
                        }
                        else
                        {
                            ret = c.Invoke(args);
                        }
                    }
                    catch (ArgumentException e)
                    {
                        Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, LOGGER);
                        StringBuilder sb = new StringBuilder("Internal Tang error?  Could not call constructor " + constructor.GetConstructorDef() + " with arguments [");
                        foreach (Object o in args)
                        {
                            sb.Append("\n\t" + o);
                        }
                        sb.Append("]");
                        Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new IllegalStateException(sb.ToString(), e), LOGGER); 
                    }
                    if (ret is IExternalConstructor<object>)
                    {
                        ret = ((IExternalConstructor<object>)ret).NewInstance();
                    }
                    instances.Add(constructor.GetNode(), ret);
                    return ret;
                }
                catch (TargetInvocationException e)
                {
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, LOGGER);
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new InjectionException("Could not invoke constructor: " + plan, e), LOGGER);
                }
                finally
                {
                    concurrentModificationGuard = false; 
                }
            }
            else if (plan is Subplan)
            {
                Subplan ambiguous = (Subplan)plan;
                return InjectFromPlan(ambiguous.GetDelegatedPlan());
            }
            else if (plan is SetInjectionPlan)
            {
                SetInjectionPlan setPlan = (SetInjectionPlan)plan;
                INode n = setPlan.GetNode();
                string typeOfSet = null;

                // TODO: This doesn't work for sets of generics (e.g., Set<Foo<int>>
                // because GetFullName and GetFullArgName strip generic info).
                if (n is INamedParameterNode)
                {
                    INamedParameterNode np = (INamedParameterNode)n;
                    typeOfSet = np.GetFullArgName();
                }
                else if (n is IClassNode)
                {
                    typeOfSet = n.GetFullName();
                }
                else
                {
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new ApplicationException("Unknown node type :" + n.ToString()), LOGGER);
                }

                Type t = classHierarchy.ClassForName(typeOfSet);
                // MakeGenericType(t = int: MonotonicHashSet<> -> MonotonicHashSet<int>
                // Get constructor: MonotonicHashSet<int> -> public MonotonicHashSet<int>() { ... }
                // Invoke: public MonotonicHashSet<int> -> new MonotonicHashSet<int>()

                object ret = typeof(MonotonicHashSet<>).MakeGenericType(t).GetConstructor(new Type[] { }).Invoke(new object[] { }); // (this, classHierarchy.ClassForName(fut.GetNode().GetFullName()));

                MethodInfo mf = ret.GetType().GetMethod("Add");

                foreach (InjectionPlan subplan in setPlan.GetEntryPlans())
                {
                    // ret.Add(InjectFromPlan(subplan));
                    mf.Invoke(ret, new object[] { InjectFromPlan(subplan) });
                }
                return ret;
            }
            else if (plan is ListInjectionPlan)
            {
                ListInjectionPlan listPlan = (ListInjectionPlan)plan;
                INode n = listPlan.GetNode();
                string typeOfList = null;

                if (n is INamedParameterNode)
                {
                    INamedParameterNode np = (INamedParameterNode)n;
                    typeOfList = np.GetFullArgName();
                }
                else if (n is IClassNode)
                {
                    typeOfList = n.GetFullName();
                }
                else
                {
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new ApplicationException("Unknown node type :" + n.ToString()), LOGGER);
                }

                Type t = classHierarchy.ClassForName(typeOfList);
                object ret = typeof(List<>).MakeGenericType(t).GetConstructor(new Type[] { }).Invoke(new object[] { }); 

                MethodInfo mf = ret.GetType().GetMethod("Add");

                foreach (InjectionPlan subplan in listPlan.GetEntryPlans())
                {
                    mf.Invoke(ret, new object[] { InjectFromPlan(subplan) });
                }
                return ret;
            }
            else
            {
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new IllegalStateException("Unknown plan type: " + plan), LOGGER);
            }
            return null; // should never reach here
        }

        private object GetCachedInstance(IClassNode cn)
        {
            if (cn.GetFullName().Equals(ReflectionUtilities.GetAssemblyQualifiedName(typeof(IInjector))))
                // if (cn.GetFullName().Equals(ReflectionUtilities.NonGenericFullName(typeof(IInjector))))
            {
                return this.ForkInjector(); // TODO: We should be insisting on injection futures here! .forkInjector();
            }
            else
            {
                object t = null;

                instances.TryGetValue(cn, out t);

                if (t != null && t is IInjectionFuture<object>)
                {
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new IllegalStateException("Found an injection future in getCachedInstance: " + cn), LOGGER);
                }
                return t;
            }
        }

        private ConstructorInfo GetConstructor(IConstructorDef constructor)
        {
            Type clazz = (Type)this.classHierarchy.ClassForName(constructor.GetClassName());
            IConstructorArg[] args = constructor.GetArgs().ToArray();
            Type[] parameterTypes = new Type[args.Length];
            for (int i = 0; i < args.Length; i++)
            {
                if (args[i].IsInjectionFuture())
                {
                    parameterTypes[i] = typeof(IInjectionFuture<>).MakeGenericType(new Type[] { this.classHierarchy.ClassForName(args[i].Gettype()) });
                }
                else
                {
                    parameterTypes[i] = this.classHierarchy.ClassForName(args[i].Gettype());
                }
            }

            ConstructorInfo cons = clazz.GetConstructor(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic, null, parameterTypes, null);
            // TODO
            // cons.setAccessible(true);

            if (cons == null)
            {
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new ApplicationException("Failed to look up constructor: " + constructor.ToString()), LOGGER);
            }
            return cons;
        }

        private void BuildInjectionPlan(INode n, IDictionary<INode, InjectionPlan> memo)
        {
            if (memo.ContainsKey(n))
            {
                InjectionPlan p = null;
                memo.TryGetValue(n, out p);
                if (BUILDING == p)
                {
                    StringBuilder loopyList = new StringBuilder("[");
                    foreach (INode node in memo.Keys)
                    {
                        InjectionPlan p1 = null;
                        memo.TryGetValue(node, out p1);
                        if (p1 == BUILDING)
                        {
                            loopyList.Append(" " + node.GetFullName());
                        }
                    }
                    loopyList.Append(" ]");
                    var ex = new ClassHierarchyException("Detected loopy constructor involving "
                        + loopyList.ToString());
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
                }
                else
                {
                    return;
                }
            }
            memo.Add(n, BUILDING);
            InjectionPlan ip = null;
            if (n is INamedParameterNode)
            {
                INamedParameterNode np = (INamedParameterNode)n;
                object boundInstance = ParseBoundNamedParameter(np);
                object defaultInstance = this.classHierarchy.ParseDefaultValue(np);
                object instance = boundInstance != null ? boundInstance : defaultInstance;

                if (instance is INode)
                {
                    BuildInjectionPlan((INode)instance, memo);
                    InjectionPlan sp;
                    memo.TryGetValue((INode)instance, out sp);
                    ip = new Subplan(n, 0, new InjectionPlan[] { sp });
                }
                else if (instance is ISet<object>)
                {
                    ISet<object> entries = (ISet<object>)instance;
                    ip = CreateSetInjectionPlan(n, memo, entries);
                }
                else if (instance is ISet<INode>)
                {
                    ISet<INode> entries = (ISet<INode>)instance;
                    ip = CreateSetInjectionPlan(n, memo, entries);
                }
                else if (instance is IList<object>)
                {
                    IList<object> entries = (IList<object>)instance;
                    ip = CreateListInjectionPlan(n, memo, entries);
                }
                else if (instance is IList<INode>)
                {
                    IList<INode> entries = (IList<INode>)instance;
                    ip = CreateListInjectionPlan(n, memo, entries);
                }
                else
                {
                    ip = new CsInstance(np, instance);
                }

            }
            else if (n is IClassNode)
            {
                IClassNode cn = (IClassNode)n;

                // Any (or all) of the next four values might be null; that's fine.
                object cached = GetCachedInstance(cn);
                IClassNode boundImpl = this.configuration.GetBoundImplementation(cn);
                IClassNode defaultImpl = ParseDefaultImplementation(cn);
                IClassNode ec = this.configuration.GetBoundConstructor(cn);

                ip = BuildClassNodeInjectionPlan(cn, cached, ec, boundImpl, defaultImpl, memo);
            }
            else if (n is IPackageNode)
            {
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new ArgumentException(
                    "Request to instantiate Java package as object"), LOGGER);
            }
            else
            {
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new IllegalStateException(
                    "Type hierarchy contained unknown node type!:" + n), LOGGER);
            }
            memo[n] = ip;
        }

        private InjectionPlan CreateSetInjectionPlan<T>(INode n, IDictionary<INode, InjectionPlan> memo, ISet<T> entries)
        {
            ISet<InjectionPlan> plans = new MonotonicHashSet<InjectionPlan>();
            CreateInjectionPlanForCollectionElements(n, memo, entries, plans);
            return new SetInjectionPlan(n, plans);
        }

        private void CreateInjectionPlanForCollectionElements<T>(INode n, IDictionary<INode, InjectionPlan> memo, ICollection<T> entries, ICollection<InjectionPlan> plans)
        {
            foreach (var entry in entries)
            {
                if (entry is IClassNode)
                {
                    BuildInjectionPlan((IClassNode)entry, memo);
                    InjectionPlan p2 = null;
                    memo.TryGetValue((INode)entry, out p2);
                    if (p2 != null)
                    {
                        plans.Add(p2);
                    }
                }
                else
                {
                    plans.Add(new CsInstance(n, entry));
                }
            }
        }

        private InjectionPlan CreateListInjectionPlan<T>(INode n, IDictionary<INode, InjectionPlan> memo, IList<T> entries)
        {
            IList<InjectionPlan> plans = new List<InjectionPlan>();
            CreateInjectionPlanForCollectionElements(n, memo, entries, plans);
            return new ListInjectionPlan(n, plans);
        }

        private InjectionPlan BuildClassNodeInjectionPlan(IClassNode cn,
            object cachedInstance,
            IClassNode externalConstructor,
            IClassNode boundImpl,
            IClassNode defaultImpl,
            IDictionary<INode, InjectionPlan> memo)
        {
            if (cachedInstance != null)
            {
                return new CsInstance(cn, cachedInstance);
            }
            else if (externalConstructor != null)
            {
                BuildInjectionPlan(externalConstructor, memo);
                InjectionPlan ip = null;
                memo.TryGetValue(externalConstructor, out ip);
                return new Subplan(cn, 0, new InjectionPlan[] { ip });
            }
            else if (boundImpl != null && !cn.Equals(boundImpl))
            {
                // We need to delegate to boundImpl, so recurse.
                BuildInjectionPlan(boundImpl, memo);
                InjectionPlan ip = null;
                memo.TryGetValue(boundImpl, out ip);
                return new Subplan(cn, 0, new InjectionPlan[] { ip });
            }
            else if (defaultImpl != null && !cn.Equals(defaultImpl))
            {
                BuildInjectionPlan(defaultImpl, memo);
                InjectionPlan ip = null;
                memo.TryGetValue(defaultImpl, out ip);
                return new Subplan(cn, 0, new InjectionPlan[] { ip });
            }
            else
            {
                // if we're here and there is a bound impl or a default impl,
                // then we're bound / defaulted to ourselves, so don't add
                // other impls to the list of things to consider.
                List<IClassNode> candidateImplementations = new List<IClassNode>();
                candidateImplementations.Add(cn);
                List<InjectionPlan> sub_ips = FilterCandidateConstructors(candidateImplementations, memo);
                if (sub_ips.Count == 1)
                {
                    return WrapInjectionPlans(cn, sub_ips, false, -1);
                }
               return WrapInjectionPlans(cn, sub_ips, true, -1);
            }
        }

        private List<InjectionPlan> FilterCandidateConstructors(
                List<IClassNode> candidateImplementations,
                IDictionary<INode, InjectionPlan> memo)
        {
            List<InjectionPlan> sub_ips = new List<InjectionPlan>();

            #region each implementation
            foreach (IClassNode thisCN in candidateImplementations)
            {
                List<InjectionPlan> constructors = new List<InjectionPlan>();
                List<IConstructorDef> constructorList = new List<IConstructorDef>();
                if (null != this.configuration.GetLegacyConstructor(thisCN))
                {
                    constructorList.Add(this.configuration.GetLegacyConstructor(thisCN));
                }

                foreach (var c in thisCN.GetInjectableConstructors())
                {
                    constructorList.Add(c);
                }

                #region each constructor
                foreach (IConstructorDef def in constructorList)
                {
                    List<InjectionPlan> args = new List<InjectionPlan>();
                    IConstructorArg[] defArgs = def.GetArgs().ToArray<IConstructorArg>();

                    #region each argument
                    foreach (IConstructorArg arg in defArgs)
                    {
                        if (!arg.IsInjectionFuture())
                        {
                            try
                            {
                                INode argNode = this.classHierarchy.GetNode(arg.GetName());
                                BuildInjectionPlan(argNode, memo);
                                InjectionPlan ip = null;
                                memo.TryGetValue(argNode, out ip);
                                args.Add(ip);
                            }
                            catch (NameResolutionException e)
                            {
                                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, LOGGER);

                                var ex = new IllegalStateException("Detected unresolvable "
                                + "constructor arg while building injection plan.  "
                                + "This should have been caught earlier!", e);
                                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
                            }
                        }
                        else
                        {
                            try
                            {
                                args.Add(new InjectionFuturePlan(this.classHierarchy.GetNode(arg.GetName())));
                            }
                            catch (NameResolutionException e)
                            {
                                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, LOGGER);
                                var ex = new IllegalStateException("Detected unresolvable "
                                + "constructor arg while building injection plan.  "
                                + "This should have been caught earlier!", e);
                                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
                            }
                        }
                    }
                    #endregion each argument

                    Constructor constructor = new Constructor(thisCN, def, args.ToArray());
                    constructors.Add(constructor);
                }
                #endregion each constructor

                // The constructors are embedded in a lattice defined by
                // isMoreSpecificThan().  We want to see if, amongst the injectable
                // plans, there is a unique dominant plan, and select it.
                // First, compute the set of injectable plans.
                List<Int32> liveIndices = new List<Int32>();
                for (int i = 0; i < constructors.Count; i++)
                {
                    if (constructors[i].GetNumAlternatives() > 0)
                    {
                        liveIndices.Add(i);
                    }
                }
                // Now, do an all-by-all comparison, removing indices that are dominated
                // by others.
                int k = -1;
                for (int i = 0; i < liveIndices.Count; i++)
                {
                    for (int j = i + 1; j < liveIndices.Count; j++)
                    {
                        IConstructorDef ci = ((Constructor)constructors[liveIndices[i]]).GetConstructorDef();
                        IConstructorDef cj = ((Constructor)constructors[liveIndices[j]]).GetConstructorDef();

                        if (ci.IsMoreSpecificThan(cj)) // ci's arguments is a superset of cj's
                        {
                            k = i;
                        }
                        else if (cj.IsMoreSpecificThan(ci))
                        {
                            k = j;
                        }
                    }
                }
                if (liveIndices.Count == 1)
                {
                    k = 0;
                }
                if (constructors.Count > 0)
                {
                    sub_ips.Add(WrapInjectionPlans(thisCN, constructors, false, k != -1 ? liveIndices[k] : -1));
                }
            }
            #endregion each implementation
            return sub_ips;
        }


        private InjectionPlan WrapInjectionPlans(IClassNode infeasibleNode,
            List<InjectionPlan> list, bool forceAmbiguous, int selectedIndex)
        {
            if (list.Count == 0)
            {
                return new Subplan(infeasibleNode, new InjectionPlan[] { });
            }
            else if ((!forceAmbiguous) && list.Count == 1)
            {
                return list[0];
            }
            else
            {
                return new Subplan(infeasibleNode, selectedIndex, list.ToArray());
            }
        }

        private IClassNode ParseDefaultImplementation(IClassNode cn)
        {
            if (cn.GetDefaultImplementation() != null)
            {
                try
                {
                    return (IClassNode)this.classHierarchy.GetNode(cn.GetDefaultImplementation());
                }
                catch (NameResolutionException e)
                {
                    var ex = new IllegalStateException("After validation, " + cn + " had a bad default implementation named " + cn.GetDefaultImplementation(), e);
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.CaughtAndThrow(ex, Level.Error, LOGGER);
                }
            }
            return null;
        }

        private object ParseBoundNamedParameter(INamedParameterNode np)
        {
            ISet<object> boundSet = this.configuration.GetBoundSet((INamedParameterNode)np);
            if (boundSet.Count != 0)
            {
                ISet<INode> ret3 = new MonotonicSet<INode>();
                ISet<object> ret2 = new MonotonicSet<object>();
                return ParseElementsInCollection(np, boundSet, ret3, ret2);
            }

            IList<object> boundList = this.configuration.GetBoundList((INamedParameterNode)np);
            if (boundList != null && boundList.Count != 0)
            {
                IList<INode> ret3 = new List<INode>();
                IList<object> ret2 = new List<object>();
                return ParseElementsInCollection(np, boundList, ret3, ret2);
            }

            object ret = null;
            if (namedParameterInstances.ContainsKey(np))
            {
                namedParameterInstances.TryGetValue(np, out ret);
            }
            else
            {
                string value = this.configuration.GetNamedParameter(np);
                if (value == null)
                {
                    ret = null;
                }
                else
                {
                    try
                    {
                        ret = this.classHierarchy.Parse(np, value);
                        namedParameterInstances.Add(np, ret);
                    }
                    catch (BindException e)
                    {
                        Org.Apache.REEF.Utilities.Diagnostics.Exceptions.CaughtAndThrow(new IllegalStateException(
                            "Could not parse pre-validated value", e), Level.Error, LOGGER); 
                    }
                }
            }
            return ret;
        }

        private object ParseElementsInCollection(INamedParameterNode np, ICollection<object> boundSet, ICollection<INode> ret3, ICollection<object> ret2)
        {
            foreach (object o in boundSet)
            {
                if (o is string)
                {
                    try
                    {
                        var r = this.classHierarchy.Parse(np, (string)o);
                        if (r is INode)
                        {
                            ret3.Add((INode)r);
                        }
                        else
                        {
                            ret2.Add(r);
                        }
                    }
                    catch (ParseException e)
                    {
                        Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, LOGGER);
                        var ex =
                            new IllegalStateException("Could not parse " + o + " which was passed into " + np +
                                                      " FIXME: Parsability is not currently checked by bindSetEntry(Node,String)");
                        Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
                    }
                }
                else if (o is INode)
                {
                    var o2 = o as INode;
                    ret3.Add(o2);
                }
                else
                {
                    var ex =
                        new IllegalStateException("Unexpected object " + o +
                                                  " in bound set.  Should consist of nodes and strings");
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
                }
            }
            if (ret2.Count > 0 && ret3.Count == 0)
            {
                return ret2;
            }
            if (ret3.Count > 0 && ret2.Count == 0)
            {
                return ret3;
            }
            if (ret2.Count > 0 && ret3.Count > 0)
            {
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new ApplicationException("Set contains different types of object"), LOGGER);
            }
            return ret2;
        }

        public object GetInstance(Type iface)
        {
            return GetInstance(ReflectionUtilities.GetAssemblyQualifiedName(iface));
        }

        public T GetInstance<T>() where T : class
        {
            return (T)GetInstance(ReflectionUtilities.GetAssemblyQualifiedName(typeof(T)));
        }

        private void CheckNamedParameter(Type t)
        {
            if (ReflectionUtilities.IsAssignableFromIgnoreGeneric(typeof(Name<>), t))
            {
                var ex = new InjectionException("GetInstance() called on Name "
                                             + ReflectionUtilities.GetName(t)
                                             + " Did you mean to call GetNamedInstance() instead?");
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
            }
        }

        private object GetInstance(INode node)
        {
            InjectionPlan plan = (InjectionPlan)GetInjectionPlan(node);
            object u = InjectFromPlan(plan);

            while (pendingFutures.Count != 0)
            {
                IEnumerator<object> i = pendingFutures.GetEnumerator();
                i.MoveNext();
                IInjectionFuture<object> f = (IInjectionFuture<object>)i.Current;
                pendingFutures.Remove(f);
                f.Get();
            }
            return u;
        }

        public object GetInstance(string clazz)
        {
            CheckNamedParameter(ReflectionUtilities.GetTypeByName(clazz));
            return GetInstance(classHierarchy.GetNode(clazz));
        }

        public U GetNamedInstance<T, U>(GenericType<T> clazz) where T : Name<U>
        {
            Type t = typeof(T);
            return (U)GetInstance(classHierarchy.GetNode(t));
        }

        public U GetNamedInstance<T, U>() where T : Name<U>
        {
            Type t = typeof(T);
            return (U)GetInstance(classHierarchy.GetNode(t));
        }

        public object GetNamedInstance(Type t)
        {
            if (!ReflectionUtilities.IsAssignableFromIgnoreGeneric(typeof(Name<>), t))
            {
                var ex = new ApplicationException(string.Format(CultureInfo.CurrentCulture, "The parameter {0} is not inherit from Name<>", t));
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
            }
            return GetInstance(classHierarchy.GetNode(t));
        }

        public InjectionPlan GetInjectionPlan(Type name)
        {
            return GetInjectionPlan(this.classHierarchy.GetNode(name));
        }

        public InjectionPlan GetInjectionPlan(INode n)
        {
            AssertNotConcurrent();
            IDictionary<INode, InjectionPlan> memo = new Dictionary<INode, InjectionPlan>();
            BuildInjectionPlan(n, memo);

            InjectionPlan p = null;
            memo.TryGetValue(n, out p);

            if (p != null)
            {
                return p;
            }
            Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new InjectionException("Fail to get injection plan" + n), LOGGER);
            return null; // this line should be not reached as Throw throws exception
        }

        public bool IsInjectable(string name)
        {
            return GetInjectionPlan(this.classHierarchy.GetNode(name)).IsInjectable();
        }

        public bool IsParameterSet(string name)
        {
            InjectionPlan p = GetInjectionPlan(classHierarchy.GetNode(name));
            return p.IsInjectable();
        }

        public bool IsInjectable(Type clazz)
        {
            AssertNotConcurrent();
            try
            {
                return IsInjectable(ReflectionUtilities.GetAssemblyQualifiedName(clazz));
            }
            catch (NameResolutionException e)
            {
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, LOGGER);
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new IllegalStateException("Could not round trip " + clazz + " through ClassHierarchy", e), LOGGER);
                return false;
            }
        }

        public bool IsParameterSet(Type name)
        {
            return IsParameterSet(ReflectionUtilities.GetAssemblyQualifiedName(name));
        }

        public void BindAspect(Aspect a)
        {
            if (aspect != null)
            {
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new BindException("Attempt to re-bind aspect! old=" + aspect + " new=" + a), LOGGER);
            }
            aspect = a;
        }

        public Aspect GetAspect()
        {
            return aspect;
        }

        public IInjector ForkInjector()
        {
            try
            {
                return ForkInjector(new ConfigurationImpl[0]);
            }
            catch (BindException e)
            {
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, LOGGER);
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new IllegalStateException("Error in forking injector.", e), LOGGER);
                return null; 
            }
        }

        public IInjector ForkInjector(params IConfiguration[] configurations)
        {
            InjectorImpl ret;
            ret = Copy(this, configurations);
            return ret;
        }

        private static InjectorImpl Copy(InjectorImpl old, IConfiguration[] configurations)
        {
            InjectorImpl injector = null;
            try
            {
                IConfigurationBuilder cb = old.configuration.newBuilder();
                foreach (IConfiguration c in configurations)
                {
                    cb.AddConfiguration(c);
                }
                injector = new InjectorImpl(cb.Build());
            }
            catch (BindException e)
            {
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, LOGGER);
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new IllegalStateException("Unexpected error copying configuration!", e), LOGGER);
            }

            foreach (IClassNode cn in old.instances.Keys)
            {
                if (cn.GetFullName().Equals(ReflectionUtilities.GetAssemblyQualifiedName(typeof(IInjector)))
                || cn.GetFullName().Equals(ReflectionUtilities.GetAssemblyQualifiedName(typeof(InjectorImpl))))
                {
                    // This would imply that we're treating injector as a singleton somewhere.  It should be copied fresh each time.
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new IllegalStateException(""), LOGGER);
                }
                try
                {
                    IClassNode new_cn = (IClassNode)injector.classHierarchy.GetNode(cn.GetFullName());

                    object o = null;
                    old.instances.TryGetValue(cn, out o);
                    if (o != null)
                    {
                        injector.instances.Add(new_cn, o);
                    }
                }
                catch (BindException e)
                {
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, LOGGER);
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new IllegalStateException("Could not resolve name "
                        + cn.GetFullName() + " when copying injector", e), LOGGER);
                }
            }

            foreach (INamedParameterNode np in old.namedParameterInstances.Keys)
            {
                // if (!builder.namedParameters.containsKey(np)) {
                Object o = null;
                old.namedParameterInstances.TryGetValue(np, out o);
                INamedParameterNode new_np = (INamedParameterNode)injector.classHierarchy.GetNode(np.GetFullName());
                injector.namedParameterInstances.Add(new_np, o);
            }

            // Fork the aspect (if any)
            if (old.aspect != null)
            {
                injector.BindAspect(old.aspect.CreateChildAspect());
            }
            return injector;
        }

        void BindVolatileInstanceNoCopy<T>(GenericType<T> c, T o) 
        {
            AssertNotConcurrent();
            INode n = this.classHierarchy.GetNode(typeof(T));
            if (n is IClassNode) 
            {
                IClassNode cn = (IClassNode)n;
                object old = GetCachedInstance(cn);
                if (old != null) {
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new BindException("Attempt to re-bind instance.  Old value was "
                    + old + " new value is " + o), LOGGER);
                }
                instances.Add(cn, o);
            } else {
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new ArgumentException("Expected Class but got " + c
                    + " (probably a named parameter)."), LOGGER);
            }
        }

        // void BindVolatileParameterNoCopy(Class<? extends Name<T>> c, T o)
        void BindVolatileParameterNoCopy<U, T>(GenericType<U> c, T o)
            where U : Name<T>
        {
            INode n = this.classHierarchy.GetNode(typeof(U));
            if (n is INamedParameterNode) 
            {
                INamedParameterNode np = (INamedParameterNode)n;
                Object old = this.configuration.GetNamedParameter(np);
                if (old != null) 
                {
                    // XXX need to get the binding site here!
                    var ex = new BindException(
                        "Attempt to re-bind named parameter " + ReflectionUtilities.GetAssemblyQualifiedName(typeof(U)) + ".  Old value was [" + old
                            + "] new value is [" + o + "]");
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
                }
                try 
                {
                    namedParameterInstances.Add(np, o);
                } 
                catch (ArgumentException e) 
                {
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Caught(e, Level.Error, LOGGER); 
                    var ex = new BindException(
                    "Attempt to re-bind named parameter " + ReflectionUtilities.GetAssemblyQualifiedName(typeof(U)) + ".  Old value was [" + old
                    + "] new value is [" + o + "]");
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
                }
            } 
            else 
            {
                var ex = new ArgumentException("Expected Name, got " + typeof(U) + " (probably a class)");
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
            }
        }

        // public <T> void bindVolatileInstance(Class<T> c, T o) throws BindException {
        public void BindVolatileInstance<T>(GenericType<T> iface, T inst)
        {
            BindVolatileInstanceNoCopy(iface, inst);
        }

        /// <summary>
        /// Binds the volatile instance.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="inst">The inst.</param>
        public void BindVolatileInstance<T>(T inst)
        {
            BindVolatileInstance<T>(GenericType<T>.Class, inst);
        }

        /// <summary>
        /// Binds the volatile parameter.
        /// </summary>
        /// <typeparam name="U"></typeparam>
        /// <typeparam name="T"></typeparam>
        /// <param name="iface">The iface.</param>
        /// <param name="inst">The inst.</param>
        public void BindVolatileParameter<U, T>(GenericType<U> iface, T inst) where U : Name<T>
        {
            BindVolatileParameterNoCopy(iface, inst);
        }

        /// <summary>
        /// Binds the volatile parameter.
        /// </summary>
        /// <typeparam name="U"></typeparam>
        /// <typeparam name="T"></typeparam>
        /// <param name="inst">The inst.</param>
        public void BindVolatileParameter<U, T>(T inst) where U : Name<T>
        {
            BindVolatileParameter(GenericType<U>.Class, inst);
        }

        ////public T GetNamedParameter<U, T>(GenericType<U> name) where U : Name<T>
        ////{
        ////   return (T)GetNamedInstance(typeof(U));
        ////}

        ////public IInjector CreateChildInjector(IConfiguration[] configurations)
        ////{
        ////   return ForkInjector(configurations);
        ////}

        /// <summary>
        /// Gets the injection plan for a given class name
        /// </summary>
        /// <param name="name">The name.</param>
        /// <returns></returns>
        public InjectionPlan GetInjectionPlan(string name)
        {
            return GetInjectionPlan(this.classHierarchy.GetNode(name));

        }
    }
}
