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

using Org.Apache.Reef.Utilities.Logging;
using System;
using System.Collections.Generic;
using System.Globalization;

namespace Org.Apache.Reef.Common
{
    public class EvaluatorHeartBeatSanityChecker
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(EvaluatorHeartBeatSanityChecker));
        
        Dictionary<string, long> _timeStamps = new Dictionary<string, long>();

        public void check(string id, long timeStamp)
        {
            lock (this)
            {
                if (_timeStamps.ContainsKey(id))
                {
                    long oldTimeStamp = _timeStamps[id];
                    LOGGER.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "TIMESTAMP CHECKER: id [{0}], old timestamp [{1}], new timestamp [{2}]", id, oldTimeStamp, timeStamp));
                    if (oldTimeStamp > timeStamp)
                    {
                        string msg = string.Format(
                            CultureInfo.InvariantCulture,
                            "Received an old heartbeat with timestamp [{0}] while timestamp [{1}] was received earlier",
                            oldTimeStamp,
                            timeStamp);
                        Org.Apache.Reef.Utilities.Diagnostics.Exceptions.Throw(new InvalidOperationException(msg), LOGGER);
                    }
                }
                _timeStamps.Add(id, timeStamp);
            }
        }
    }
}
