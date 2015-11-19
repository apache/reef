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
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Tang.Examples
{
    public class MyEventStreamDefinition
    {
        public ISet<int> Shards { get; set; }

        public Type Type { get; set; }
    }

    public class AnonymousType
    {
        private readonly Dictionary<string, MyEventStreamDefinition> d = new Dictionary<string, MyEventStreamDefinition>();
        private Dictionary<string, int> d2;

        // Anonymous class in injectable constructor
        [Inject]
        public AnonymousType()
        {
            d2 = d
                .Select((e, i) => new { e.Key, i })
                .ToDictionary(e => e.Key, e => e.i);
        }

        // Anonymous class in other constructor
        public AnonymousType(Dictionary<string, MyEventStreamDefinition> d)
        {
            this.d = d;            
            d2 = d
                .Select((e, i) => new { e.Key, i })
                .ToDictionary(e => e.Key, e => e.i);
        }
    }
}