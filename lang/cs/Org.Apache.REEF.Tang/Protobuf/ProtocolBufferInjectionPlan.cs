// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

using System.Collections.Generic;
using System.IO;
using System.Linq;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Implementations.InjectionPlan;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Types;
using Org.Apache.REEF.Utilities.Logging;
using ProtoBuf;

namespace Org.Apache.REEF.Tang.Protobuf
{
    internal sealed class ProtocolBufferInjectionPlan
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(ProtocolBufferInjectionPlan));

        private static InjectionPlan NewConstructor(string fullName, List<InjectionPlan> plans) 
        {
            Constructor cconstr = new Constructor();
            foreach (InjectionPlan p in plans)
            {
                cconstr.args.Add(p);
            }

            InjectionPlan plan = new InjectionPlan();
            plan.name = fullName;
            plan.constructor = cconstr;
            return plan;
        }

        private static InjectionPlan NewSubplan(string fullName, int selectedPlan, List<InjectionPlan> plans) 
        {
            Subplan subPlan = new Subplan();

            subPlan.selected_plan = selectedPlan;
            foreach (InjectionPlan p in plans)
            {
                subPlan.plans.Add(p);
            }

            InjectionPlan plan = new InjectionPlan();
            plan.name = fullName;
            plan.subplan = subPlan;
            return plan;
        }

        private static InjectionPlan NewInstance(string fullName, string value)
        {
            Instance instance = new Instance();
            instance.value = value;

            InjectionPlan plan = new InjectionPlan();
            plan.name = fullName;
            plan.instance = instance;
            return plan;
        }

        public static void Serialize(string fileName, Implementations.InjectionPlan.InjectionPlan ip)
        {
            InjectionPlan plan = Serialize(ip);

            using (var file = File.Create(fileName))
            {
                Serializer.Serialize<InjectionPlan>(file, plan);
            }
        }

        public static InjectionPlan Serialize(Implementations.InjectionPlan.InjectionPlan ip) 
        {
            if (ip is Implementations.InjectionPlan.Constructor) 
            {
                Implementations.InjectionPlan.Constructor cons = (Implementations.InjectionPlan.Constructor)ip;
                Implementations.InjectionPlan.InjectionPlan[] args = cons.GetArgs();
                InjectionPlan[] protoArgs = new InjectionPlan[args.Length];
                for (int i = 0; i < args.Length; i++) 
                {
                    protoArgs[i] = Serialize(args[i]);
                }
                return NewConstructor(ip.GetNode().GetFullName(), protoArgs.ToList<InjectionPlan>());
            } 
            if (ip is Implementations.InjectionPlan.Subplan) 
            {
                Implementations.InjectionPlan.Subplan sp = (Implementations.InjectionPlan.Subplan)ip;
                Implementations.InjectionPlan.InjectionPlan[] args = sp.GetPlans();
                InjectionPlan[] subPlans = new InjectionPlan[args.Length];
                for (int i = 0; i < args.Length; i++) 
                {
                    subPlans[i] = Serialize(args[i]);
                }
                return NewSubplan(ip.GetNode().GetFullName(), sp.GetSelectedIndex(), subPlans.ToList<InjectionPlan>());
            } 
            if (ip is CsInstance) 
            {
                CsInstance ji = (CsInstance)ip;
                return NewInstance(ip.GetNode().GetFullName(), ji.GetInstanceAsString());
            } 
            Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new IllegalStateException(
                    "Encountered unknown type of InjectionPlan: " + ip), LOGGER);
            return null;
        }

        public static Implementations.InjectionPlan.InjectionPlan DeSerialize(string fileName, IClassHierarchy ch)
        {
            InjectionPlan protoPlan;

            using (var file = File.OpenRead(fileName))
            {
                protoPlan = Serializer.Deserialize<InjectionPlan>(file);
            }

            return Deserialize(ch, protoPlan);
        }

        public static Implementations.InjectionPlan.InjectionPlan Deserialize(IClassHierarchy ch, InjectionPlan ip) 
        {
            string fullName = ip.name;
            if (ip.constructor != null) 
            {
                Constructor cons = ip.constructor;
                IClassNode cn = (IClassNode)ch.GetNode(fullName);

                InjectionPlan[] protoBufArgs = cons.args.ToArray();

                IClassNode[] cnArgs = new IClassNode[protoBufArgs.Length];

                for (int i = 0; i < protoBufArgs.Length; i++) 
                {
                    INode no = ch.GetNode(protoBufArgs[i].name);
                    if (no is IClassNode)
                    {
                        cnArgs[i] = (IClassNode)no;
                    }
                    else if (no is INamedParameterNode)
                    {
                        INamedParameterNode np = (INamedParameterNode)no;
                        cnArgs[i] = (IClassNode)ch.GetNode(np.GetFullArgName());
                    }
                }

                Implementations.InjectionPlan.InjectionPlan[] ipArgs = new Implementations.InjectionPlan.InjectionPlan[protoBufArgs.Length];

                for (int i = 0; i < protoBufArgs.Length; i++) 
                {
                    ipArgs[i] = (Implementations.InjectionPlan.InjectionPlan)Deserialize(ch, protoBufArgs[i]);
                }

                IConstructorDef constructor = cn.GetConstructorDef(cnArgs);
                return new Implementations.InjectionPlan.Constructor(cn, constructor, ipArgs);
            }
            if (ip.instance != null) 
            {
                Instance ins = ip.instance;
                object instance = Parse(ip.name, ins.value);
                return new CsInstance(ch.GetNode(ip.name), instance);
            } 
            if (ip.subplan != null) 
            {
                Subplan subplan = ip.subplan;
                InjectionPlan[] protoBufPlans = subplan.plans.ToArray();

                Implementations.InjectionPlan.InjectionPlan[] subPlans = new Implementations.InjectionPlan.InjectionPlan[protoBufPlans.Length];
                for (int i = 0; i < protoBufPlans.Length; i++) 
                {
                    subPlans[i] = (Implementations.InjectionPlan.InjectionPlan)Deserialize(ch, protoBufPlans[i]);
                }
                INode n = ch.GetNode(fullName);
                return new Implementations.InjectionPlan.Subplan(n, subPlans);
            } 
            Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new IllegalStateException("Encountered unknown type of InjectionPlan: " + ip), LOGGER);
            return null;
        }

        private static object Parse(string type, string value)
        {
            // XXX this is a placeholder for now.  We need a parser API that will
            // either produce a live java object or (partially) validate stuff to
            // see if it looks like the target language will be able to handle this
            // type + value.
            return value;
        }
    }
}
