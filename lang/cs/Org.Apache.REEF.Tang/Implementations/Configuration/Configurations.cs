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

using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Protobuf;

namespace Org.Apache.REEF.Tang.Implementations.Configuration
{
    public static class Configurations
    {
        public static IConfiguration Merge(params IConfiguration[] configurations) 
        {
            return TangFactory.GetTang().NewConfigurationBuilder(configurations).Build();
        }

        public static IConfiguration MergeDeserializedConfs(params IConfiguration[] configurations)
        {
            IClassHierarchy ch; 

            if (configurations != null && configurations.Length > 0)
            {
                ch = configurations[0].GetClassHierarchy();
            }
            else
            {
                ch = new ProtocolBufferClassHierarchy();               
            }
           
            IConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder(ch);

            foreach (IConfiguration tc in configurations)
            {
                cb.AddConfiguration((ConfigurationImpl)tc);
            }

            return cb.Build();
        }
    }
}
