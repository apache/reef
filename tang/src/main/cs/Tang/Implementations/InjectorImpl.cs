using System;
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
        private ICsClassHierarchy classHierarchy;
        readonly IDictionary<IClassNode, Object> instances = new MonotonicTreeMap<IClassNode, Object>();
        private bool concurrentModificationGuard = false;
        private Aspect aspect;
        private readonly ISet<InjectionFuture> pendingFutures = new HashSet<InjectionFuture>();

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
                        ret = ((IExternalConstructor)ret).newInstance();
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
            if (cn.GetFullName().Equals("om.Microsoft.Tang.Interface.IInjector")) 
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

        public object GetInstance(Type iface)
        {
            return GetInstance(iface.FullName);
        }

        private object GetInstance(INode node)
        {
            throw new NotImplementedException();
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
            throw new NotImplementedException();
        }

        public bool IsInjectable(string name)
        {
            throw new NotImplementedException();
        }

        public bool IsParameterSet(string name)
        {
            throw new NotImplementedException();
        }

        public bool IsInjectable(Type clazz)
        {
            throw new NotImplementedException();
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
    }
}
