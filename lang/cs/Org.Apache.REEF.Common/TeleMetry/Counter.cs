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
using System.Runtime.Serialization;
using Newtonsoft.Json;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Common.Telemetry
{
    /// <summary>
    /// Counter implementation
    /// The properties that need to be serialized will be revisited later. We should only serialize minimum data to reduce the network load
    /// For example, the name can be mapped to a unique number (byte) and description should not be serialized.
    /// </summary>
    [Unstable("0.16", "This is a simple counter for evaluator metrics.")]
    [DataContract]
    internal sealed class Counter : ICounter
    {
        /// <summary>
        /// Name of the counter.
        /// </summary>
        private readonly string _name;

        /// <summary>
        /// Description of the counter.
        /// </summary>
        private readonly string _description;

        /// <summary>
        /// Time that the counter is updated.
        /// </summary>
        private long _timeStamp;

        /// <summary>
        /// Value of the counter.
        /// </summary>
        private int _value;

        /// <summary>
        /// Constructor to create a new counter.
        /// </summary>
        /// <param name="name"></param>
        /// <param name="description"></param>
        internal Counter(string name, string description)
        {
            _name = name;
            _description = description;
            _timeStamp = DateTime.Now.Ticks;
            _value = 0;
        }

        /// <summary>
        /// Constructor to create a counter from a serialized counter string
        /// </summary>
        [JsonConstructor]
        internal Counter(string name, string description, long timeStamp, int value)
        {
            _name = name;
            _description = description;
            _timeStamp = timeStamp;
            _value = value;
        }

        /// <summary>
        /// Description of the counter.
        /// </summary>
        [DataMember]
        public string Description
        {
            get
            {
                return _description;
            }
        }

        /// <summary>
        /// Name of the counter.
        /// </summary>
        [DataMember]
        public string Name
        {
            get
            {
                return _name;
            }
        }

        /// <summary>
        /// Time that the counter is updated in the form of ticks.
        /// </summary>
        [DataMember]
        public long Timestamp
        {
            get
            {
                return _timeStamp;
            }
        }

        /// <summary>
        /// Value of the counter.
        /// </summary>
        [DataMember]
        public int Value
        {
            get
            {
                return _value;
            }
        }

        /// <summary>
        /// Increase the counter value and update the time stamp.
        /// </summary>
        /// <param name="number"></param>
        public void Increment(int number)
        {
            _value += number;
            _timeStamp = DateTime.Now.Ticks;
        }
    }
}
