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

namespace Org.Apache.REEF.Common.Telemetry
{
    [DataContract]
    public class Counter : ICounter
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
        public Counter(string name, string description)
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
        public Counter(string name, string description, long timeStamp, int value)
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
        /// Time that the counter is updated.
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
