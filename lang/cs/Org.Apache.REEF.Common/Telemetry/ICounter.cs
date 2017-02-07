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

using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Common.Telemetry
{
    [Unstable("0.16", "This is a simple counter for evaluator metrics.")]
    public interface ICounter
    {
        /// <summary>
        /// Time the counter is updated.
        /// </summary>
        long Timestamp { get; }

        /// <summary>
        /// Name of the counter.
        /// </summary>
        string Name { get; }

        /// <summary>
        /// The description of the counter.
        /// </summary>
        string Description { get; }

        /// <summary>
        /// The value of the counter.
        /// </summary>
        int Value { get; }

        /// <summary>
        /// Increase the current counter value with the number specified.
        /// </summary>
        void Increment(int number);
    }
}
