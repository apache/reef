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

using System.Collections.Generic;
using Org.Apache.REEF.IMRU.API;

namespace Org.Apache.REEF.IMRU.InProcess
{
    /// <summary>
    /// Helper class to hold a set of mappers.
    /// </summary>
    /// <typeparam name="TMapInput"></typeparam>
    /// <typeparam name="TMapOutput"></typeparam>
    /// This is needed as Tang doesn't support BindVolatile into a Set.
    internal sealed class MapFunctions<TMapInput, TMapOutput>
    {
        private readonly ISet<IMapFunction<TMapInput, TMapOutput>> _mappers;

        internal MapFunctions(ISet<IMapFunction<TMapInput, TMapOutput>> mappers)
        {
            _mappers = mappers;
        }

        internal ISet<IMapFunction<TMapInput, TMapOutput>> Mappers
        {
            get { return _mappers; }
        }
    }
}