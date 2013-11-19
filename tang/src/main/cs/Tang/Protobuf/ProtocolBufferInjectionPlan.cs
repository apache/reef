using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Com.Microsoft.Tang.Exceptions;
using Com.Microsoft.Tang.Implementations;
using Com.Microsoft.Tang.Interface;
using Com.Microsoft.Tang.Types;
using ProtoBuf;

namespace Com.Microsoft.Tang.Protobuf
{
    public class ProtocolBufferInjectionPlan
    {

        private InjectionPlanProto.InjectionPlan NewConstructor(string fullName, List<InjectionPlanProto.InjectionPlan> plans) 
        {
            InjectionPlanProto.Constructor cconstr = new InjectionPlanProto.Constructor();
            foreach (InjectionPlanProto.InjectionPlan p in plans)
            {
                cconstr.args.Add(p);
            }

            InjectionPlanProto.InjectionPlan plan = new InjectionPlanProto.InjectionPlan();
            plan.name = fullName;
            plan.constructor = cconstr;
            return plan;
        }

        private InjectionPlanProto.InjectionPlan NewSubplan(string fullName, int selectedPlan, List<InjectionPlanProto.InjectionPlan> plans) 
        {
            InjectionPlanProto.Subplan subPlan = new InjectionPlanProto.Subplan();

            subPlan.selected_plan = selectedPlan;
            foreach (InjectionPlanProto.InjectionPlan p in plans)
            {
                subPlan.plans.Add(p);
            }

            InjectionPlanProto.InjectionPlan plan = new InjectionPlanProto.InjectionPlan();
            plan.name = fullName;
            plan.subplan = subPlan;
            return plan;
        }

        private InjectionPlanProto.InjectionPlan NewInstance(string fullName, string value)
        {
            InjectionPlanProto.Instance instance = new InjectionPlanProto.Instance();
            instance.value = value;

            InjectionPlanProto.InjectionPlan plan = new InjectionPlanProto.InjectionPlan();
            plan.name = fullName;
            plan.instance = instance;
            return plan;

        }

        public void Serialize(string fileName, InjectionPlan ip)
        {
            InjectionPlanProto.InjectionPlan plan = Serialize(ip);

            using (var file = File.Create(fileName))
            {
                Serializer.Serialize<InjectionPlanProto.InjectionPlan>(file, plan);
            }
        }

        public InjectionPlanProto.InjectionPlan Serialize(InjectionPlan ip) 
        {
            if (ip is Constructor) 
            {
                Constructor cons = (Constructor) ip;
                InjectionPlan[] args = cons.getArgs();
                InjectionPlanProto.InjectionPlan[] protoArgs = new InjectionPlanProto.InjectionPlan[args.Length];
                for (int i = 0; i < args.Length; i++) 
                {
                    protoArgs[i] = Serialize(args[i]);
                }
                return NewConstructor(ip.GetNode().GetFullName(), protoArgs.ToList<InjectionPlanProto.InjectionPlan>());
            } 
            else if (ip is Subplan) 
            {
                Subplan sp = (Subplan) ip;
                InjectionPlan[] args = sp.GetPlans();
                InjectionPlanProto.InjectionPlan[] subPlans = new InjectionPlanProto.InjectionPlan[args.Length];
                for (int i = 0; i < args.Length; i++) 
                {
                    subPlans[i] = Serialize(args[i]);
                }
                return NewSubplan(ip.GetNode().GetFullName(), sp.GetSelectedIndex(), subPlans.ToList<InjectionPlanProto.InjectionPlan>());

            } 
            else if (ip is CsInstance) 
            {
                CsInstance ji = (CsInstance) ip;
                return NewInstance(ip.GetNode().GetFullName(), ji.GetInstanceAsString());
            } else 
            {
                throw new IllegalStateException(
                    "Encountered unknown type of InjectionPlan: " + ip);
            }
        }

        public InjectionPlan DeSerialize(string fileName, IClassHierarchy ch)
        {
            InjectionPlanProto.InjectionPlan protoPlan;

            using (var file = File.OpenRead(fileName))
            {
                protoPlan = Serializer.Deserialize<InjectionPlanProto.InjectionPlan>(file);
            }

            return Deserialize(ch, protoPlan);
        }

        public InjectionPlan Deserialize(IClassHierarchy ch, InjectionPlanProto.InjectionPlan ip) 
        {
            string fullName = ip.name;
            if (ip.constructor != null) 
            {
                InjectionPlanProto.Constructor cons = ip.constructor;
                IClassNode cn = (IClassNode) ch.GetNode(fullName);

                InjectionPlanProto.InjectionPlan[] protoBufArgs = cons.args.ToArray();

                IClassNode[] cnArgs = new IClassNode[protoBufArgs.Length];

                for (int i = 0; i < protoBufArgs.Length; i++) 
                {
                    cnArgs[i] = (IClassNode) ch.GetNode(protoBufArgs[i].name);
                }

                InjectionPlan[] ipArgs = new InjectionPlan[protoBufArgs.Length];

                for (int i = 0; i < protoBufArgs.Length; i++) 
                {
                    ipArgs[i] = (InjectionPlan) Deserialize(ch, protoBufArgs[i]);
                }

                IConstructorDef constructor = cn.GetConstructorDef(cnArgs);
                return new Constructor(cn, constructor, ipArgs);
            }
            else if (ip.instance != null) 
            {
                InjectionPlanProto.Instance ins = ip.instance;
                object instance = Parse(ip.name, ins.value);
                return new CsInstance(ch.GetNode(ip.name), instance);
            } 
            else if (ip.subplan != null) 
            {
                InjectionPlanProto.Subplan subplan = ip.subplan;
                InjectionPlanProto.InjectionPlan[] protoBufPlans = subplan.plans.ToArray();
          
                InjectionPlan[] subPlans = new InjectionPlan[protoBufPlans.Length];
                for (int i = 0; i < protoBufPlans.Length; i++) 
                {
                    subPlans[i] = (InjectionPlan) Deserialize(ch, protoBufPlans[i]);
                }
                INode n = ch.GetNode(fullName);
                return new Subplan(n, subPlans);
            } 
            else 
            {
                throw new IllegalStateException("Encountered unknown type of injection plan: " + ip);
            }
        }

        private object Parse(String type, String value)
        {
            // XXX this is a placeholder for now.  We need a parser API that will
            // either produce a live java object or (partially) validate stuff to
            // see if it looks like the target language will be able to handle this
            // type + value.
            return value;
        }
    }
}
