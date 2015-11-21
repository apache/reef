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

using System.Collections.Generic;
using System.Globalization;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.Tang.Tests.SmokeTest
{
    class TestConfigurationModuleBuilder : ConfigurationModuleBuilder
    {
        public static readonly string RequiredStringValue = "Required String Value";

        public static readonly string OptionalStringValue = "Optional String Value";

        public static readonly RequiredParameter<string> REQUIREDSTRING = new RequiredParameter<string>();

        public static readonly OptionalParameter<string> OPTIONALSTRING = new OptionalParameter<string>();

        public static readonly int NamedParameterIntegerValue = 42;

        public static readonly double NamedParameterDoubleValue = 42.0;

        public static ConfigurationModule CONF
        {
            get
            {
                return new TestConfigurationModuleBuilder()
                    .BindImplementation(GenericType<IRootInterface>.Class, GenericType<RootImplementation>.Class)
                    .BindNamedParameter<NamedParameterInteger, int>(GenericType<NamedParameterInteger>.Class, NamedParameterIntegerValue.ToString(CultureInfo.CurrentCulture))
                    .BindNamedParameter<NamedParameterDouble, double>(GenericType<NamedParameterDouble>.Class, NamedParameterDoubleValue.ToString(CultureInfo.CurrentCulture))
                    .BindSetEntry<SetOfInstances, SetInterfaceImplOne, ISetInterface>(GenericType<SetOfInstances>.Class, GenericType<SetInterfaceImplOne>.Class)
                    .BindSetEntry<SetOfInstances, SetInterfaceImplTwo, ISetInterface>(GenericType<SetOfInstances>.Class, GenericType<SetInterfaceImplTwo>.Class)
                    .BindNamedParameter(GenericType<RequiredString>.Class, REQUIREDSTRING)
                    .BindNamedParameter(GenericType<OptionalString>.Class, OPTIONALSTRING)
                    .BindSetEntry<SetOfBaseTypes.Integers, int>(GenericType<SetOfBaseTypes.Integers>.Class, "1")
                    .BindSetEntry<SetOfBaseTypes.Integers, int>(GenericType<SetOfBaseTypes.Integers>.Class, "2")
                    .BindSetEntry<SetOfBaseTypes.Integers, int>(GenericType<SetOfBaseTypes.Integers>.Class, "3")
                    .BindSetEntry<SetOfBaseTypes.Doubles, double>(GenericType<SetOfBaseTypes.Doubles>.Class, "1")
                    .BindSetEntry<SetOfBaseTypes.Doubles, double>(GenericType<SetOfBaseTypes.Doubles>.Class, "2")
                    .BindSetEntry<SetOfBaseTypes.Doubles, double>(GenericType<SetOfBaseTypes.Doubles>.Class, "3")
                    .BindSetEntry<SetOfBaseTypes.Strings, string>(GenericType<SetOfBaseTypes.Strings>.Class, "1")
                    .BindSetEntry<SetOfBaseTypes.Strings, string>(GenericType<SetOfBaseTypes.Strings>.Class, "2")
                    .BindSetEntry<SetOfBaseTypes.Strings, string>(GenericType<SetOfBaseTypes.Strings>.Class, "3")
                    .BindList<ListOfBaseTypes.Integers, int>(GenericType<ListOfBaseTypes.Integers>.Class, new List<string>(new string[] { "1", "2", "3" }))
                    .BindList<ListOfBaseTypes.Doubles, double>(GenericType<ListOfBaseTypes.Doubles>.Class, new List<string>(new string[] { "1", "2", "3" }))
                    .BindList<ListOfBaseTypes.Strings, string>(GenericType<ListOfBaseTypes.Strings>.Class, new List<string>(new string[] { "1", "2", "3" }))
                    .Build();
            }
        }

        [NamedParameter]
        public class RequiredString : Name<string>
        {
        }

        [NamedParameter]
        public class SetOfInstances : Name<ISet<ISetInterface>>
        {
        }

        [NamedParameter]
        public class NamedParameterDouble : Name<double>
        {
        }

        [NamedParameter]
        public class IntegerHandler : Name<IHandler<int>>
        {
        }

        [NamedParameter]
        public class StringHandler : Name<IHandler<string>>
        {
        }

        [NamedParameter]
        public class NamedParameterInteger : Name<int>
        {
        }

        [NamedParameter(DefaultValue = "default_string_default_value")]
        public class OptionalString : Name<string>
        {
        }
    }
}
