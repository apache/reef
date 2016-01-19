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

using System;
using System.Collections.Generic;
using System.Text;
using Org.Apache.REEF.Tang.Types;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Tang.Implementations.InjectionPlan
{
    // contains the data for injecting a Node such as which Constructor to use, what are the arguments
    public abstract class InjectionPlan : ITraversable<InjectionPlan> 
    {
        protected INode node;

        private static readonly Logger LOGGER = Logger.GetLogger(typeof(InjectionPlan));

        public InjectionPlan(INode node) 
        {
            this.node = node;
        }

        public INode GetNode() 
        {
            return node;
        }

        /// <summary>
        /// Get child elements of the injection plan tree. By default, returns an empty list.
        /// </summary>
        /// <returns>An empty list</returns>
        public ICollection<InjectionPlan> GetChildren()
        {
            return new List<InjectionPlan>();
        }

        public abstract int GetNumAlternatives();

        public bool IsFeasible()
        {
            return GetNumAlternatives() > 0;
        }

        abstract public bool IsAmbiguous();

        abstract public bool IsInjectable();

        //// abstract public bool HasFutureDependency();

        protected void pad(StringBuilder sb, int n)
        {
            for (int i = 0; i < n; i++)
            {
                sb.Append("  ");
            }
        }

        private static void Newline(StringBuilder pretty, int indent)
        {
            pretty.Append('\n');
            for (int j = 0; j < indent * 2; j++)
            {
                pretty.Append(' ');
            }
        }

        public string ToPrettyString()
        {
            string ugly = node.GetFullName() + ":\n" + ToString();
            StringBuilder pretty = new StringBuilder();
            int currentIndent = 1;
            for (int i = 0; i < ugly.Length; i++)
            {
                char c = ugly[i];
                if (c == '(')
                {
                    if (ugly[i + 1] == ')')
                    {
                        pretty.Append("()");
                        i++;
                    }
                    else
                    {
                        Newline(pretty, currentIndent);
                        currentIndent++;
                        pretty.Append(c);
                        pretty.Append(' ');
                    }
                }
                else if (c == '[')
                {
                    if (ugly[i + 1] == ']')
                    {
                        pretty.Append("[]");
                        i++;
                    }
                    else
                    {
                        Newline(pretty, currentIndent);
                        currentIndent++;
                        pretty.Append(c);
                        pretty.Append(' ');
                    }
                }
                else if (c == ')' || c == ']')
                {
                    currentIndent--;
                    Newline(pretty, currentIndent);
                    pretty.Append(c);
                }
                else if (c == '|')
                {
                    Newline(pretty, currentIndent);
                    pretty.Append(c);
                }
                else if (c == ',')
                {
                    currentIndent--;
                    Newline(pretty, currentIndent);
                    pretty.Append(c);
                    currentIndent++;
                }
                else
                {
                    pretty.Append(c);
                }
            }
            return pretty.ToString();
        }

        public string ToCantInjectString() 
        {
            if (!IsFeasible()) 
            {
                return ToInfeasibleInjectString();
            } 
            if (IsAmbiguous()) 
            {
                return ToAmbiguousInjectString();
            } 
            var ex = new ArgumentException(
                "toCantInjectString() called on injectable constructor:"
                + this.ToPrettyString());
            Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
            return null;
        }

        public abstract string ToAmbiguousInjectString();

        public abstract string ToInfeasibleInjectString();

        public abstract bool IsInfeasibleLeaf();

        public abstract string ToShallowString();
    }

    public class BuildingInjectionPlan : InjectionPlan
    {
        public BuildingInjectionPlan(INode node)
            : base(node)
        {
        }

        public override int GetNumAlternatives()
        {
            throw new NotSupportedException();
        }

        public override bool IsAmbiguous()
        {
            throw new NotSupportedException();
        }

        public override bool IsInjectable()
        {
            throw new NotSupportedException();
        }

        ////public override bool HasFutureDependency()
        ////{
        ////    throw new NotSupportedException();
        ////}

        public override string ToAmbiguousInjectString()
        {
            throw new NotSupportedException();
        }

        public override string ToInfeasibleInjectString()
        {
            throw new NotSupportedException();
        }

        public override bool IsInfeasibleLeaf()
        {
            throw new NotSupportedException();
        }

        public override string ToShallowString()
        {
            throw new NotSupportedException();
        }
    }
}
