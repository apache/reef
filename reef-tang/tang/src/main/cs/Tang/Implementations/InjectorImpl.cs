/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Com.Microsoft.Tang.Exceptions;
using Com.Microsoft.Tang.Interface;
using Com.Microsoft.Tang.Types;
using Com.Microsoft.Tang.Util;

namespace Com.Microsoft.Tang.Implementations
{
    public class InjectorImpl : IInjector
    {
        IDictionary<INamedParameterNode, object> namedParameterInstances = new MonotonicTreeMap<INamedParameterNode, object>();
        private ICsClassHierarchy classHierarchy;
        private IConfiguration configuration;
        readonly IDictionary<IClassNode, Object> instances = new MonotonicTreeMap<IClassNode, Object>();
        private bool concurrentModificationGuard = false;
        private Aspect aspect;
        private readonly ISet<InjectionFuture> pendingFutures = new HashSet<InjectionFuture>();

        static readonly InjectionPlan BUILDING = new BuildingInjectionPlan(null); //TODO anonymous class


        public InjectorImpl(IConfiguration c) 
        {
            this.configuration = c;
            this.classHierarchy = (ICsClassHierarchy)c.GetClassHierarchy();
        }

        public object InjectFromPlan(InjectionPlan plan)
        {

            if (!plan.IsFeasible()) 
            {
                throw new InjectionException("Cannot inject " + plan.GetNode().GetFullName() + ": "
                    + plan.ToCantInjectString());
            }
            if (plan.IsAmbiguous())
            {
                throw new InjectionException("Cannot inject " + plan.GetNode().GetFullName() + " "
                    + plan.ToCantInjectString());
            }
            
            if (plan is InjectionFuturePlan) 
            {
                InjectionFuturePlan fut = (InjectionFuturePlan)plan;
                string key = fut.GetNode().GetFullName();
                try
                {
                    //we will see if we need to introduce T to replace object
                    InjectionFuture ret = new InjectionFuture(this, classHierarchy.ClassForName(fut.GetNode().GetFullName()));
                    pendingFutures.Add(ret);
                    return ret;
                }
                catch (TypeLoadException e)
                {
                    throw new InjectionException("Could not get class for " + key);
                }
            }
            else if(plan.GetNode() is IClassNode && null != GetCachedInstance((IClassNode)plan.GetNode())) 
            {
                return GetCachedInstance((IClassNode)plan.GetNode());
            } 
            else if (plan is CsInstance) 
            {
                // TODO: Must be named parameter node.  Check.
                //      throw new IllegalStateException("Instance from plan not in Injector's set of instances?!?");
                return ((CsInstance) plan).instance;
            } 
            else if (plan is Constructor) 
            {
                Constructor constructor = (Constructor) plan;
                object[] args = new object[constructor.GetArgs().Length];
                InjectionPlan[] argPlans = constructor.GetArgs();

                for (int i = 0; i < argPlans.Length; i++) 
                {
                    args[i] = InjectFromPlan(argPlans[i]);
                }

                try
                {
                    concurrentModificationGuard = true;
                    object ret;
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
                        StringBuilder sb = new StringBuilder("Internal Tang error?  Could not call constructor " + constructor.GetConstructorDef() + " with arguments [");
                        foreach (Object o in args)
                        {
                            sb.Append("\n\t" + o);
                        }
                        sb.Append("]");
                        throw new IllegalStateException(sb.ToString(), e);
                    }
                    if (ret is IExternalConstructor)
                    {
                        ret = ((IExternalConstructor)ret).NewInstance();
                    }
                    instances.Add(constructor.GetNode(), ret);
                    return ret;
                }
                catch (Exception e) 
                {
                    throw new InjectionException("Could not invoke constructor: " + plan,  e); //check what exception might be got and refine it
                } finally 
                {
                    concurrentModificationGuard = false;
                }
            }
            else if (plan is Subplan) 
            {
                Subplan ambiguous = (Subplan) plan;
                return InjectFromPlan(ambiguous.GetDelegatedPlan());
            } 
            else if (plan is SetInjectionPlan) 
            {
                SetInjectionPlan setPlan = (SetInjectionPlan) plan;
                ISet<object> ret = new MonotonicSet<object>();
                foreach (InjectionPlan subplan in setPlan.GetEntryPlans()) 
                {
                    ret.Add(InjectFromPlan(subplan));
                }
                return ret;
            } else 
            {
                throw new IllegalStateException("Unknown plan type: " + plan);
            }
        }

        private object GetCachedInstance(IClassNode cn) 
        {
            if (cn.GetFullName().Equals("com.Microsoft.Tang.Interface.IInjector")) 
            {
                return this;// TODO: We should be insisting on injection futures here! .forkInjector();
            } 
            else 
            {
                object t = null;

                instances.TryGetValue(cn, out t);

                if (t != null && t is InjectionFuture)
                {
                    throw new IllegalStateException("Found an injection future in getCachedInstance: " + cn);
                }
                return t; 
            }
        }

        private ConstructorInfo GetConstructor(IConstructorDef constructor)  
        {
            Type clazz = (Type) this.classHierarchy.ClassForName(constructor.GetClassName());
            IConstructorArg[] args = constructor.GetArgs().ToArray();
            Type[] parameterTypes= new Type[args.Length];
            for (int i = 0; i < args.Length; i++) 
            {
                if (args[i].IsInjectionFuture()) 
                {
                    parameterTypes[i] = typeof(InjectionFuture);  
                } 
                else 
                {
                    parameterTypes[i] = this.classHierarchy.ClassForName(args[i].Gettype());
                }
            }

            ConstructorInfo cons = clazz.GetConstructor(parameterTypes);
            //cons.setAccessible(true);
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
                        if(p1 == BUILDING) 
                        {
                            loopyList.Append(" " + node.GetFullName());
                        }
                    }
                    loopyList.Append(" ]");
                    throw new ClassHierarchyException("Detected loopy constructor involving "
                        + loopyList.ToString());
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
                    InjectionPlan sp = null;
                    memo.TryGetValue((INode)instance, out sp);
                    if (sp != null)
                    {
                        ip = new Subplan(n, 0, new InjectionPlan[] {sp});
                    }
                }
                else if(instance is ISet<object>) 
                {
                    ISet<object> entries = (ISet<object>) instance;
                    ISet<InjectionPlan> plans = new MonotonicSet<InjectionPlan>();
                    foreach (object entry in entries) 
                    {
                        if(entry is IClassNode) 
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
                    ip = new SetInjectionPlan(n, plans);
                } 
                else 
                {
                    ip = new CsInstance(np, instance);
                }
                               
            }
            else if (n is IClassNode) 
            {
                IClassNode cn = (IClassNode) n;

                // Any (or all) of the next four values might be null; that's fine.
                object cached = GetCachedInstance(cn);
                IClassNode boundImpl = this.configuration.GetBoundImplementation(cn);
                IClassNode defaultImpl = ParseDefaultImplementation(cn);
                IClassNode ec = this.configuration.GetBoundConstructor(cn);
      
                ip = BuildClassNodeInjectionPlan(cn, cached, ec, boundImpl, defaultImpl, memo);
            } else if (n is IPackageNode) 
            {
                throw new ArgumentException(
                    "Request to instantiate Java package as object");
            } else 
            {
                throw new IllegalStateException(
                    "Type hierarchy contained unknown node type!:" + n);
            }
            memo[n] = ip;

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
            else {
                // if we're here and there is a bound impl or a default impl,
                // then we're bound / defaulted to ourselves, so don't add
                // other impls to the list of things to consider.
                List<IClassNode> candidateImplementations = new List<IClassNode>();
                if (boundImpl == null && defaultImpl == null) 
                {
                    foreach (var ki in cn.GetKnownImplementations())
                    candidateImplementations.Add(ki);
                }
                candidateImplementations.Add(cn);
                List<InjectionPlan> sub_ips = FilterCandidateConstructors(candidateImplementations, memo);
                if (candidateImplementations.Count == 1 && candidateImplementations[0].GetFullName().Equals(cn.GetFullName())) 
                {
                    return WrapInjectionPlans(cn, sub_ips, false, -1);
                } 
                else 
                {
                    return WrapInjectionPlans(cn, sub_ips, true, -1);
                }
            }
        }

         private List<InjectionPlan> FilterCandidateConstructors(
                List<IClassNode> candidateImplementations,
                IDictionary<INode, InjectionPlan> memo) 
         {

            List<InjectionPlan> sub_ips = new List<InjectionPlan>();
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
        

                foreach (IConstructorDef def in constructorList) 
                {
                    List<InjectionPlan> args = new List<InjectionPlan>();
                    IConstructorArg[] defArgs = def.GetArgs().ToArray<IConstructorArg>();

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
                                throw new IllegalStateException("Detected unresolvable "
                                + "constructor arg while building injection plan.  "
                                + "This should have been caught earlier!", e);
                            }
                        } 
                        else 
                        {
                            try 
                            {
                                args.Add(new InjectionFuturePlan(this.classHierarchy.GetNode(arg.GetName())));
                            } catch (NameResolutionException e) {
                                throw new IllegalStateException("Detected unresolvable "
                                + "constructor arg while building injection plan.  "
                                + "This should have been caught earlier!", e);
                            }
                        }
                    }
                    Constructor constructor = new Constructor(thisCN, def, args.ToArray());
                    constructors.Add(constructor);
                }

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
                for (int i = 0; i < liveIndices.Count; i++) 
                {
                    for (int j = i + 1; j < liveIndices.Count; j++) 
                    {
                        IConstructorDef ci = ((Constructor)constructors[(liveIndices[i])]).GetConstructorDef();
                        IConstructorDef cj = ((Constructor)constructors[(liveIndices[j])]).GetConstructorDef();

                        if (ci.IsMoreSpecificThan(cj)) 
                        {
                            liveIndices.Remove(j);
                            j--;
                        } 
                        else if (cj.IsMoreSpecificThan(ci)) 
                        {
                            liveIndices.Remove(j);
                            // Done with this inner loop invocation. Check the new ci.
                            i--;
                            break;
                        }
                    }
                }
                sub_ips.Add(WrapInjectionPlans(thisCN, constructors, false, liveIndices.Count == 1 ? liveIndices[0] : -1));
            }
            return sub_ips;
        }

        private InjectionPlan WrapInjectionPlans(IClassNode infeasibleNode,
            List<InjectionPlan> list, bool forceAmbiguous, int selectedIndex) 
        {
            if (list.Count == 0) 
            {
                return new Subplan(infeasibleNode, new InjectionPlan[]{});
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

        private  IClassNode ParseDefaultImplementation(IClassNode cn) 
        {
            if (cn.GetDefaultImplementation() != null) 
            {
                try 
                {
                    return (IClassNode)this.classHierarchy.GetNode(cn.GetDefaultImplementation());
                } 
                catch( NameResolutionException e) 
                {
                    throw new IllegalStateException("After validation, " + cn + " had a bad default implementation named " + cn.GetDefaultImplementation(), e);
                }
            } 
            else 
            {
                return null;
            }
        }

        private object ParseBoundNamedParameter(INamedParameterNode np) 
        {
            object ret;
            ISet<Object> boundSet = this.configuration.GetBoundSet((INamedParameterNode)np);
            if (boundSet.Count != 0) 
            {
                ISet<object> ret2 = new MonotonicSet<object>();
                foreach (Object o in boundSet) 
                {
                    if(o is string) 
                    {
                        try 
                        {
                            ret2.Add(this.classHierarchy.Parse(np, (string)o));
                        } 
                        catch(ParseException e) 
                        {
                            throw new IllegalStateException("Could not parse " + o + " which was passed into " + np + " FIXME: Parsability is not currently checked by bindSetEntry(Node,String)");
                        }
                    } 
                    else if(o is INode) 
                    {
                        ret2.Add(o);
                    } 
                    else 
                    {
                        throw new IllegalStateException("Unexpected object " + o + " in bound set.  Should consist of nodes and strings");
                    }
                }

                return ret2;
            }
            else if (namedParameterInstances.ContainsKey(np))
            {
                namedParameterInstances.TryGetValue(np, out ret);
            }
            else 
            {
                string value = this.configuration.GetNamedParameter(np);
                if(value == null) 
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
                        throw new IllegalStateException(
                            "Could not parse pre-validated value", e);
                    }
                }
            }
            return ret;
        } 

        public object GetInstance(Type iface)
        {
            return GetInstance(iface.FullName);
        }

        private object GetInstance(INode node)
        {
            InjectionPlan plan = (InjectionPlan)GetInjectionPlan(node);
            object u = InjectFromPlan(plan);
    
            //while(pendingFutures.Count != 0) 
            //{
            //    Iterator<InjectionFuture<?>> i = pendingFutures.iterator();
            //    InjectionFuture f = i.next();
            //    pendingFutures.remove(f);
            //    f.Get();
            //}
            return u;
        }

        public object GetInstance(string clazz)
        {
            return GetInstance(classHierarchy.GetNode(clazz));
        }

        public object GetNamedInstance(Type iface)
        {
            throw new NotImplementedException();
        }

        public InjectionPlan GetInjectionPlan(Type name)
        {
            return GetInjectionPlan(this.classHierarchy.GetNode(name));
        }

        public InjectionPlan GetInjectionPlan(INode n) 
        {
            //assertNotConcurrent();
            IDictionary<INode, InjectionPlan> memo = new Dictionary<INode, InjectionPlan>();
            BuildInjectionPlan(n, memo);

            InjectionPlan p = null;
            memo.TryGetValue(n, out p);

            if (p != null)
            {
                return p;
            }
            throw new InjectionException("Fail to get injection plan" + n);
        }

        public bool IsInjectable(string name)
        {
           return GetInjectionPlan(this.classHierarchy.GetNode(name)).IsInjectable();
        }

        public bool IsParameterSet(string name)
        {
            throw new NotImplementedException();
        }

        public bool IsInjectable(Type clazz)
        {
            //assertNotConcurrent();
            try
            {
                return IsInjectable(clazz.FullName);
            }
            catch (NameResolutionException e)
            {
                throw new IllegalStateException("Could not round trip " + clazz + " through ClassHierarchy", e);
            }
        }

        public bool isParameterSet(Type name)
        {
            throw new NotImplementedException();
        }

        public void BindAspect(Aspect a) 
        {
            if(aspect != null) 
            {
                throw new BindException("Attempt to re-bind aspect! old=" + aspect + " new=" + a);
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
                throw new IllegalStateException(e.Message);
            }
        }

        public IInjector ForkInjector(IConfiguration[] configurations)
        {
            InjectorImpl ret;
            ret = Copy(this, configurations);
            return ret;
        }

        private static InjectorImpl Copy(InjectorImpl old, IConfiguration[] configurations)  
        {
            InjectorImpl i;
            try 
            {
                IConfigurationBuilder cb = old.configuration.newBuilder();
                foreach (IConfiguration c in configurations) 
                {
                    cb.AddConfiguration(c);
                }
                i = new InjectorImpl(cb.Build());
            } 
            catch (BindException e) 
            {
                throw new IllegalStateException("Unexpected error copying configuration!", e);
            }

            foreach (IClassNode cn in old.instances.Keys) 
            {
                if (cn.GetFullName().Equals(ReflectionUtilities.GetFullName(typeof(IInjector)))
                || cn.GetFullName().Equals(ReflectionUtilities.GetFullName(typeof(InjectorImpl)))) 
                {
                    // This would imply that we're treating injector as a singleton somewhere.  It should be copied fresh each time.
                    throw new IllegalStateException("");
                }
                try 
                {
                    IClassNode new_cn = (IClassNode) i.classHierarchy.GetNode(cn.GetFullName());
                    
                    object o = null;
                    old.instances.TryGetValue(cn, out o);
                    if (o != null)
                    {
                        i.instances.Add(new_cn, o);
                    }
                } 
                catch (BindException e) 
                {
                    throw new IllegalStateException("Could not resolve name "
                        + cn.GetFullName() + " when copying injector", e);
                }
            }

            foreach (INamedParameterNode np in old.namedParameterInstances.Keys) 
            {
                // if (!builder.namedParameters.containsKey(np)) {
                Object o = null;
                old.namedParameterInstances.TryGetValue(np, out o);
                INamedParameterNode new_np = (INamedParameterNode) i.classHierarchy.GetNode(np.GetFullName());
                i.namedParameterInstances.Add(new_np, o);
            }

            // Fork the aspect (if any)
            if (old.aspect != null)
            {
                i.BindAspect(old.aspect.CreateChildAspect());
            }
            return i;
        }
    }
}
