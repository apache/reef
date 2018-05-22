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

namespace Org.Apache.REEF.Common.Telemetry
{
    class StringMetric : MetricBase<string>
    {
        public override bool IsImmutable
        {
            get { return false; }
        }

        public StringMetric(string name, string description)
            : base(name, description)
        {
        }

        internal StringMetric(string name, string description, long timeStamp, string value)
            : base(name, description, timeStamp, value)
        {
        }

        public override IMetric CreateInstanceWithNewValue(object val)
        {
            return new StringMetric(Name, Description, DateTime.Now.Ticks, (string)val);
        }
    }
}
