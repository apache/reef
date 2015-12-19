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
using System.Globalization;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Common.Catalog.Capabilities
{
    public sealed class CPU : ICapability
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(CPU));

        private readonly int _cores;

        public CPU(int cores)
        {
            if (cores <= 0)
            {
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new ArgumentException("cores cannot be non-positive"), LOGGER);
            }
            _cores = cores;
        }

        public int Cores
        {
            get
            {
                return _cores;
            }
        }

        public override string ToString()
        {
            return string.Format(CultureInfo.InvariantCulture, "CPU Cores = [{0}]", Cores);
        }

        public override int GetHashCode()
        {
            return Cores.GetHashCode();
        }
    }
}
