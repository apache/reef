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
    public class ProtocolBufferInjectionPlan
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(ProtocolBufferInjectionPlan));

        private static Org.Apache.REEF.Tang.Protobuf.InjectionPlan NewConstructor(string fullName, List<Org.Apache.REEF.Tang.Protobuf.InjectionPlan> plans) 
        {
            Org.Apache.REEF.Tang.Protobuf.Constructor cconstr = new Org.Apache.REEF.Tang.Protobuf.Constructor();
            foreach (Org.Apache.REEF.Tang.Protobuf.InjectionPlan p in plans)
            {
                cconstr.args.Add(p);
            }

            Org.Apache.REEF.Tang.Protobuf.InjectionPlan plan = new Org.Apache.REEF.Tang.Protobuf.InjectionPlan();
            plan.name = fullName;
            plan.constructor = cconstr;
            return plan;
        }

        private static Org.Apache.REEF.Tang.Protobuf.InjectionPlan NewSubplan(string fullName, int selectedPlan, List<Org.Apache.REEF.Tang.Protobuf.InjectionPlan> plans) 
        {
            Org.Apache.REEF.Tang.Protobuf.Subplan subPlan = new Org.Apache.REEF.Tang.Protobuf.Subplan();

            subPlan.selected_plan = selectedPlan;
            foreach (Org.Apache.REEF.Tang.Protobuf.InjectionPlan p in plans)
            {
                subPlan.plans.Add(p);
            }

            Org.Apache.REEF.Tang.Protobuf.InjectionPlan plan = new Org.Apache.REEF.Tang.Protobuf.InjectionPlan();
            plan.name = fullName;
            plan.subplan = subPlan;
            return plan;
        }

        private static Org.Apache.REEF.Tang.Protobuf.InjectionPlan NewInstance(string fullName, string value)
        {
            Org.Apache.REEF.Tang.Protobuf.Instance instance = new Org.Apache.REEF.Tang.Protobuf.Instance();
            instance.value = value;

            Org.Apache.REEF.Tang.Protobuf.InjectionPlan plan = new Org.Apache.REEF.Tang.Protobuf.InjectionPlan();
            plan.name = fullName;
            plan.instance = instance;
            return plan;

        }

        public static void Serialize(string fileName, Org.Apache.REEF.Tang.Implementations.InjectionPlan.InjectionPlan ip)
        {
            Org.Apache.REEF.Tang.Protobuf.InjectionPlan plan = Serialize(ip);

            using (var file = File.Create(fileName))
            {
                Serializer.Serialize<Org.Apache.REEF.Tang.Protobuf.InjectionPlan>(file, plan);
            }
        }

        public static Org.Apache.REEF.Tang.Protobuf.InjectionPlan Serialize(Org.Apache.REEF.Tang.Implementations.InjectionPlan.InjectionPlan ip) 
        {
            if (ip is Org.Apache.REEF.Tang.Implementations.InjectionPlan.Constructor) 
            {
                Org.Apache.REEF.Tang.Implementations.InjectionPlan.Constructor cons = (Org.Apache.REEF.Tang.Implementations.InjectionPlan.Constructor)ip;
                Org.Apache.REEF.Tang.Implementations.InjectionPlan.InjectionPlan[] args = cons.GetArgs();
                Org.Apache.REEF.Tang.Protobuf.InjectionPlan[] protoArgs = new Org.Apache.REEF.Tang.Protobuf.InjectionPlan[args.Length];
                for (int i = 0; i < args.Length; i++) 
                {
                    protoArgs[i] = Serialize(args[i]);
                }
                return NewConstructor(ip.GetNode().GetFullName(), protoArgs.ToList<Org.Apache.REEF.Tang.Protobuf.InjectionPlan>());
            } 
            if (ip is Org.Apache.REEF.Tang.Implementations.InjectionPlan.Subplan) 
            {
                Org.Apache.REEF.Tang.Implementations.InjectionPlan.Subplan sp = (Org.Apache.REEF.Tang.Implementations.InjectionPlan.Subplan)ip;
                Org.Apache.REEF.Tang.Implementations.InjectionPlan.InjectionPlan[] args = sp.GetPlans();
                Org.Apache.REEF.Tang.Protobuf.InjectionPlan[] subPlans = new Org.Apache.REEF.Tang.Protobuf.InjectionPlan[args.Length];
                for (int i = 0; i < args.Length; i++) 
                {
                    subPlans[i] = Serialize(args[i]);
                }
                return NewSubplan(ip.GetNode().GetFullName(), sp.GetSelectedIndex(), subPlans.ToList<Org.Apache.REEF.Tang.Protobuf.InjectionPlan>());

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

        public static Org.Apache.REEF.Tang.Implementations.InjectionPlan.InjectionPlan DeSerialize(string fileName, IClassHierarchy ch)
        {
            Org.Apache.REEF.Tang.Protobuf.InjectionPlan protoPlan;

            using (var file = File.OpenRead(fileName))
            {
                protoPlan = Serializer.Deserialize<Org.Apache.REEF.Tang.Protobuf.InjectionPlan>(file);
            }

            return Deserialize(ch, protoPlan);
        }

        public static Org.Apache.REEF.Tang.Implementations.InjectionPlan.InjectionPlan Deserialize(IClassHierarchy ch, Org.Apache.REEF.Tang.Protobuf.InjectionPlan ip) 
        {
            string fullName = ip.name;
            if (ip.constructor != null) 
            {
                Org.Apache.REEF.Tang.Protobuf.Constructor cons = ip.constructor;
                IClassNode cn = (IClassNode)ch.GetNode(fullName);

                Org.Apache.REEF.Tang.Protobuf.InjectionPlan[] protoBufArgs = cons.args.ToArray();

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

                Org.Apache.REEF.Tang.Implementations.InjectionPlan.InjectionPlan[] ipArgs = new Org.Apache.REEF.Tang.Implementations.InjectionPlan.InjectionPlan[protoBufArgs.Length];

                for (int i = 0; i < protoBufArgs.Length; i++) 
                {
                    ipArgs[i] = (Org.Apache.REEF.Tang.Implementations.InjectionPlan.InjectionPlan)Deserialize(ch, protoBufArgs[i]);
                }

                IConstructorDef constructor = cn.GetConstructorDef(cnArgs);
                return new Org.Apache.REEF.Tang.Implementations.InjectionPlan.Constructor(cn, constructor, ipArgs);
            }
            if (ip.instance != null) 
            {
                Org.Apache.REEF.Tang.Protobuf.Instance ins = ip.instance;
                object instance = Parse(ip.name, ins.value);
                return new CsInstance(ch.GetNode(ip.name), instance);
            } 
            if (ip.subplan != null) 
            {
                Org.Apache.REEF.Tang.Protobuf.Subplan subplan = ip.subplan;
                Org.Apache.REEF.Tang.Protobuf.InjectionPlan[] protoBufPlans = subplan.plans.ToArray();

                Org.Apache.REEF.Tang.Implementations.InjectionPlan.InjectionPlan[] subPlans = new Org.Apache.REEF.Tang.Implementations.InjectionPlan.InjectionPlan[protoBufPlans.Length];
                for (int i = 0; i < protoBufPlans.Length; i++) 
                {
                    subPlans[i] = (Org.Apache.REEF.Tang.Implementations.InjectionPlan.InjectionPlan)Deserialize(ch, protoBufPlans[i]);
                }
                INode n = ch.GetNode(fullName);
                return new Org.Apache.REEF.Tang.Implementations.InjectionPlan.Subplan(n, subPlans);
            } 
            Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new IllegalStateException("Encountered unknown type of InjectionPlan: " + ip), LOGGER);
            return null;
        }

        private static object Parse(String type, String value)
        {
            // XXX this is a placeholder for now.  We need a parser API that will
            // either produce a live java object or (partially) validate stuff to
            // see if it looks like the target language will be able to handle this
            // type + value.
            return value;
        }
    }
}
