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
using System.Linq;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Network.Group.Operators.Impl
{
    [Private]
    public sealed class ReduceFunction<T> : IReduceFunction<T>
    {
        private readonly Func<T, T, T> _reduceFunction;
        private readonly T _initialValue;
 
        private ReduceFunction(Func<T, T, T> reduceFunction)
        {
            _reduceFunction = reduceFunction;
        }

        private ReduceFunction(Func<T, T, T> reduceFunction, T initialValue)
        {
            _reduceFunction = reduceFunction;
            _initialValue = initialValue;
        }

        public static IReduceFunction<T> Create(Func<T, T, T> reduceFunction)
        {
            return new ReduceFunction<T>(reduceFunction);
        }

        public static IReduceFunction<T> Create(Func<T, T, T> reduceFunction, T initialValue)
        {
            return new ReduceFunction<T>(reduceFunction, initialValue);
        }

        public T Reduce(IEnumerable<T> elements)
        {
            if (_initialValue == null)
            {
                return elements.Aggregate(_reduceFunction);
            }

            return elements.Aggregate(_initialValue, _reduceFunction);
        }
    }
}
