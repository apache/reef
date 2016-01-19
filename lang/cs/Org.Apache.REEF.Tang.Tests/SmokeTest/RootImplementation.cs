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

using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Tang.Tests.SmokeTest
{
    public class RootImplementation : IRootInterface
    {
        private readonly string requiredString;
        private readonly string optionalString;
        private readonly IAnInterface anInterface;
        private readonly int anInt;
        private readonly double aDouble;
        private readonly InjectableClass injectableClass;
        private readonly SetOfImplementations setOfImplementations;
        private readonly SetOfBaseTypes setOfBaseTypes;
        //// private readonly ListOfBaseTypes listOfBaseTypes;  // TODO: to recover once Avro NuGet support it
        private readonly CyclicDependency cyclicDependency;

        /// <param name="requiredString">required string</param>
        /// <param name="optionalString">optional string</param>
        /// <param name="anInt">an int</param>
        /// <param name="aDouble">a double</param>
        /// <param name="anInterface">an interface</param>
        /// <param name="injectableClass">an injectable class</param>
        /// <param name="setOfImplementations">set of implementations</param>
        /// <param name="setOfBaseTypes">set of base types</param>
        /// <param name="cyclicDependency">cyclic dependency</param>
        [Inject]
        public RootImplementation([Parameter(typeof(TestConfigurationModuleBuilder.RequiredString))] string requiredString,
                                  [Parameter(typeof(TestConfigurationModuleBuilder.OptionalString))] string optionalString,
                                  [Parameter(typeof(TestConfigurationModuleBuilder.NamedParameterInteger))] int anInt,
                                  [Parameter(typeof(TestConfigurationModuleBuilder.NamedParameterDouble))] double aDouble,
                                  IAnInterface anInterface,
                                  InjectableClass injectableClass,
                                  SetOfImplementations setOfImplementations,
                                  SetOfBaseTypes setOfBaseTypes,
                                  CyclicDependency cyclicDependency)
        {
                                this.requiredString = requiredString;
                                this.optionalString = optionalString;
                                this.anInterface = anInterface;
                                this.anInt = anInt;
                                this.aDouble = aDouble;
                                this.injectableClass = injectableClass;
                                this.setOfImplementations = setOfImplementations;
                                this.setOfBaseTypes = setOfBaseTypes;
                                //// this.listOfBaseTypes = listOfBaseTypes;  // TODO: to recover once Avro NuGet support it
                                this.cyclicDependency = cyclicDependency;
        }

        public bool IsValid()
        {
            if (!this.setOfImplementations.isValid())
            {
                return false;
            }

            if (!this.requiredString.Equals(TestConfigurationModuleBuilder.RequiredStringValue))
            {
                return false;
            }

            if (!this.optionalString.Equals(TestConfigurationModuleBuilder.OptionalStringValue))
            {
                return false;
            }

            if (this.anInterface == null)
            {
                return false;
            }

            if (this.aDouble != TestConfigurationModuleBuilder.NamedParameterDoubleValue)
            {
                return false;
            }

            if (this.anInt != TestConfigurationModuleBuilder.NamedParameterIntegerValue)
            {
                return false;
            }

            return true;
        }

        public override bool Equals(object o)
        {
            if (this == o)
            {
                return true;
            }

            if (o == null)
            {
                return false;
            }

            RootImplementation that = (RootImplementation)o;
            
            if (that.aDouble != aDouble)
            {
                return false;
            }

            if (anInt != that.anInt)
            {
                return false;
            }

            if (anInterface != null ? !anInterface.Equals(that.anInterface) : that.anInterface != null)
            {
                return false;
            }

            if (optionalString != null ? !optionalString.Equals(that.optionalString) : that.optionalString != null)
            {
                return false;
            }

            if (requiredString != null ? !requiredString.Equals(that.requiredString) : that.requiredString != null)
            {
                return false;
            }

            if (injectableClass != null ? !injectableClass.Equals(that.injectableClass) : that.injectableClass != null)
            {
                return false;
            }

            if (setOfImplementations != null
                    ? !setOfImplementations.Equals(that.setOfImplementations)
                    : that.setOfImplementations != null)
            {
                return false;
            }

            if (setOfBaseTypes != null ? !setOfBaseTypes.Equals(that.setOfBaseTypes) : that.setOfBaseTypes != null)
            {
                return false;
            }

            ////TODO: to recover once Avro NuGet support it
            ////if (listOfBaseTypes != null ? !listOfBaseTypes.Equals(that.listOfBaseTypes) : that.listOfBaseTypes != null)
            ////{
            ////   return false;
            ////}
            if (cyclicDependency != null
                    ? !cyclicDependency.Equals(that.cyclicDependency)
                    : that.cyclicDependency != null)
            {
                return false;
            }

            return true;
        }

        public override int GetHashCode() 
        {
            int result;
            result = requiredString != null ? requiredString.GetHashCode() : 0;
            result = (31 * result) + (optionalString != null ? optionalString.GetHashCode() : 0);
            result = (31 * result) + (anInterface != null ? anInterface.GetHashCode() : 0);
            result = (31 * result) + anInt;
            result = (31 * result) + aDouble.GetHashCode();
            return result;
       }
    }
}
