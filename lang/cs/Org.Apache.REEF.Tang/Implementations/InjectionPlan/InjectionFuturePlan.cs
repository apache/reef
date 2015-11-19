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
using Org.Apache.REEF.Tang.Types;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Tang.Implementations.InjectionPlan
{
    public class InjectionFuturePlan : InjectionPlan
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(InjectionFuturePlan));

        public InjectionFuturePlan(INode name) : base(name)
        {
        }

        public override int GetNumAlternatives()
        {
            return 1;
        }

        public override bool IsAmbiguous()
        {
            return false;
        }

        public override bool IsInjectable()
        {
            return true;
        }

        ////public override bool HasFutureDependency()
        ////{
        ////   return true;
        ////}

        public override string ToAmbiguousInjectString()
        {
            throw new NotSupportedException("InjectionFuturePlan cannot be ambiguous!");
        }

        public override string ToInfeasibleInjectString()
        {
            throw new NotSupportedException("InjectionFuturePlan is always feasible!");
        }

        public override bool IsInfeasibleLeaf()
        {
            return false;
        }

        public override string ToShallowString()
        {
            return "InjectionFuture<" + GetNode().GetFullName() + ">";
        }
    }
}
