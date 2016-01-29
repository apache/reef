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
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Types;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Tang.Implementations.ClassHierarchy
{
    internal sealed class ClassNodeImpl : AbstractNode, IClassNode
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(ClassNodeImpl));

        private readonly bool injectable;
        private readonly bool unit;
        private readonly bool externalConstructor;
        private readonly IList<IConstructorDef> injectableConstructors;
        private readonly IList<IConstructorDef> allConstructors;
        private readonly MonotonicSet<IClassNode> knownImpls;
        private readonly string defaultImpl;

        public ClassNodeImpl(INode parent, string simpleName, string fullName,
            bool unit, bool injectable, bool externalConstructor,
            IList<IConstructorDef> injectableConstructors,
            IList<IConstructorDef> allConstructors,
            string defaultImplementation)
            : base(parent, simpleName, fullName)
        {
            this.unit = unit;
            this.injectable = injectable;
            this.externalConstructor = externalConstructor;
            this.injectableConstructors = injectableConstructors;
            this.allConstructors = allConstructors;
            this.knownImpls = new MonotonicSet<IClassNode>();
            this.defaultImpl = defaultImplementation;
        }

        public IList<IConstructorDef> GetInjectableConstructors()
        {
            return injectableConstructors;
        }

        public IList<IConstructorDef> GetAllConstructors()
        {
            return allConstructors;
        }

        public bool IsInjectionCandidate()
        {
            return injectable;
        }

        public bool IsExternalConstructor()
        {
            return externalConstructor;
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder(base.ToString() + ": ");
            if (GetInjectableConstructors() != null)
            {
                foreach (IConstructorDef c in GetInjectableConstructors())
                {
                    sb.Append(c.ToString() + ", ");
                }
            }
            else
            {
                sb.Append("OBJECT BUILD IN PROGRESS!  BAD NEWS!");
            }
            return sb.ToString();
        }

        public IConstructorDef GetConstructorDef(IList<IClassNode> paramTypes)
        {
            if (!IsInjectionCandidate())
            {
                var e = new BindException("Cannot @Inject non-static member/local class: "
                + GetFullName());
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(e, LOGGER);
            }
            foreach (IConstructorDef c in GetAllConstructors())
            {
                if (c.TakesParameters(paramTypes))
                {
                    return c;
                }
            }
            Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new BindException("Could not find requested constructor for class " + GetFullName()), LOGGER);
            return null; // this line would not be reached as Thrwo throws an exception 
        }

        public void PutImpl(IClassNode impl)
        {
            knownImpls.Add(impl);
        }

        public ISet<IClassNode> GetKnownImplementations()
        {
            return new MonotonicSet<IClassNode>(knownImpls);
        }

        public bool IsUnit()
        {
            return unit;
        }

        public bool IsImplementationOf(IClassNode inter)
        {
            List<IClassNode> worklist = new List<IClassNode>();
            if (this.Equals(inter))
            {
                return true;
            }
            worklist.Add(inter);
            while (worklist.Count != 0)
            {
                IClassNode cn = worklist[worklist.Count - 1];
                worklist.RemoveAt(worklist.Count - 1);
                ISet<IClassNode> impls = cn.GetKnownImplementations();
                if (impls.Contains(this))
                {
                    return true;
                }
                worklist.AddRange(impls);
            }
            return false;
        }

        public string GetDefaultImplementation()
        {
            return defaultImpl;
        }
    }
}
