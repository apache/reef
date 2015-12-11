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

namespace Org.Apache.REEF.Tang.Implementations.ClassHierarchy
{
    public class ConstructorArgImpl : IConstructorArg
    {
        private readonly String type;
        private readonly String name;
        private readonly bool isInjectionFuture;

        public ConstructorArgImpl(String type, String namedParameterName, bool isInjectionFuture)
        {
            this.type = type;
            this.name = namedParameterName;

            ////if (name != null && name.Contains(','))
            ////   throw new ApplicationException("Name contains comma : " + name);
            this.isInjectionFuture = isInjectionFuture;
        }

        public string GetName()
        {
            return name == null ? type : name;
        }

        public string GetNamedParameterName()
        {
            return name;
        }

        public string Gettype()
        {
            return type;
        }

        public bool IsInjectionFuture()
        {
            return isInjectionFuture;
        }

        public override String ToString()
        {
            return name == null ? type : type + " " + name;
        }

        public override int GetHashCode()
        {
            return 0;
        }

        public override bool Equals(Object o)
        {
            ConstructorArgImpl arg = (ConstructorArgImpl)o;
            if (!type.Equals(arg.type))
            {
                return false;
            }
            if (name == null && arg.name == null)
            {
                return true;
            }
            if (name == null && arg.name != null)
            {
                return false;
            }
            if (name != null && arg.name == null)
            {
                return false;
            }
            return name.Equals(arg.name);
        }
    }
}
