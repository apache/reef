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
using Org.Apache.REEF.Tang.Types;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Tang.Implementations.InjectionPlan
{
    internal sealed class CsInstance : InjectionPlan
    {
        public readonly object instance;
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(CsInstance));

        public CsInstance(INode name, object instance) : base(name)
        {
            this.instance = instance;
        }
        public override int GetNumAlternatives()
        {
            return instance == null ? 0 : 1;
        }

        public override string ToString()
        {
            return GetNode() + " = " + instance;
        }

        public override bool IsAmbiguous()
        {
            return false;
        }

        public override bool IsInjectable()
        {
            return instance != null;
        }

        public string GetInstanceAsString()
        {
            return instance.ToString();
        }

        ////public override bool HasFutureDependency()
        ////{
        ////    return false;
        ////}

        public override string ToAmbiguousInjectString()
        {
            var ex = new ArgumentException("toAmbiguousInjectString called on CsInstance!" + this.ToString());
            Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(ex, LOGGER);
            return null;
        }

        public override string ToInfeasibleInjectString()
        {
            return GetNode() + " is not bound.";
        }

        public override bool IsInfeasibleLeaf()
        {
            return true;
        }

        public override string ToShallowString()
        {
            return ToString();
        }
    }
}
