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
using System.Linq;
using System.Text;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Types;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Tang.Implementations.ClassHierarchy
{
    public class ConstructorDefImpl : IConstructorDef
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(ConstructorDefImpl));

        private readonly IList<IConstructorArg> args = new List<IConstructorArg>();
        private readonly String className;

        public ConstructorDefImpl(String className, IConstructorArg[] args, bool injectable)
        {
            this.args = args;
            this.className = className;
            if (injectable)
            {
                var duplicateItems = from x in args
                                     group x by x into grouped
                                     where grouped.Count() > 1
                                     select grouped.Key;

                if (duplicateItems.Any())
                {
                    var e = new ClassHierarchyException(
                        "Repeated constructor parameter detected.  "
                        + "Cannot inject constructor " + ToString());
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
                }
            }
        }

        public IList<IConstructorArg> GetArgs()
        {
            return args;
        }

        public String GetClassName()
        {
            return className;
        }

        private String Join(String sep, Object[] vals)
        {
            if (vals.Length != 0)
            {
                StringBuilder sb = new StringBuilder(vals[0].ToString());
                for (int i = 1; i < vals.Length; i++)
                {
                    sb.Append(sep + vals[i]);
                }
                return sb.ToString();
            }
            else
            {
                return "";
            }
        }

        public override String ToString()
        {
            StringBuilder sb = new StringBuilder(className);
            sb.Append("(");
            sb.Append(Join(",", args.ToArray()));
            sb.Append(")");
            return sb.ToString();
        }

        // Return true if our list of args is a superset of those in def.
        public bool IsMoreSpecificThan(IConstructorDef def)
        {
            // Is everything in def also in this?
            for (int i = 0; i < def.GetArgs().Count; i++)
            {
                bool found = false;
                for (int j = 0; j < this.GetArgs().Count; j++)
                {
                    if (GetArgs()[j].Equals(def.GetArgs()[i]))
                    {
                        found = true;
                        break;
                    }
                }
                // If not, then argument j from def is not in our list.  Return false.
                if (found == false)
                    return false;
            }
            // Everything in def's arg list is in ours.  Do we have at least one extra
            // argument?
            return GetArgs().Count > def.GetArgs().Count;
        }

        public bool TakesParameters(IList<IClassNode> paramTypes)
        {
            if (paramTypes.Count != args.Count)
            {
                return false;
            }

            int i = 0;
            foreach (INode t in paramTypes)
            {
                string s;
                if (t is INamedParameterNode)
                {
                    s = ((INamedParameterNode)paramTypes[i]).GetFullArgName();
                }
                else
                {
                    s = paramTypes[i].GetFullName();
                }
                if (!args[i].Gettype().Equals(s))
                {
                    return false;
                }
                else
                {
                    i++;
                }

            }
            return true;
        }

        public override bool Equals(Object o)
        {
            return EqualsIgnoreOrder((IConstructorDef)o);
        }

        public override int GetHashCode()
        {
            return 0;
        }

        // A(int i, string j) vs. A(string i, int j) is Ambiguous in injection
        private bool EqualsIgnoreOrder(IConstructorDef def)
        {
            if (GetArgs().Count != def.GetArgs().Count)
            {
                return false;
            }
            for (int i = 0; i < GetArgs().Count; i++)
            {
                bool found = false;
                for (int j = 0; j < def.GetArgs().Count; j++)
                {
                    if (GetArgs()[i].GetName().Equals(def.GetArgs()[j].GetName()))
                    {
                        found = true;
                    }
                }
                if (!found)
                {
                    return false;
                }
            }
            return true;
        }

        public int CompareTo(object obj)
        {
            IConstructorDef o = (IConstructorDef)obj;
            return ToString().CompareTo(o.ToString());
        }

        public bool IsInList(IList<IConstructorDef> list)
        {
            foreach (IConstructorDef def in list)
            {
                if (CompareTo(def) == 0)
                {
                    return true;
                }

            }
            return false;
        }
    }
}
