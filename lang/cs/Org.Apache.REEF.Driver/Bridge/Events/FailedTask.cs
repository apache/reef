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
using System.Runtime.Serialization;
using Org.Apache.REEF.Driver.Bridge.Clr2java;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Driver.Bridge.Events
{
    internal sealed class FailedTask : IFailedTask
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(FailedTask));
        
        public FailedTask(IFailedTaskClr2Java failedTaskClr2Java)
        {
            InstanceId = Guid.NewGuid().ToString("N");
            Parse(failedTaskClr2Java);
            FailedTaskClr2Java = failedTaskClr2Java;
            ActiveContextClr2Java = failedTaskClr2Java.GetActiveContext();
        }

        public Optional<string> Reason { get; set; }

        [DataMember]
        public string InstanceId { get; set; }

        public string Id { get; private set; }

        public string Message { get; set; }

        public Optional<string> Description { get; set; }

        public Optional<Exception> Cause { get; set; }

        public Optional<byte[]> Data { get; set; }

        [DataMember]
        private IFailedTaskClr2Java FailedTaskClr2Java { get; set; }

        [DataMember]
        private IActiveContextClr2Java ActiveContextClr2Java { get; set; }

        /// <summary>
        /// Access the context the task ran (and crashed) on, if it could be recovered.
        /// An ActiveContext is given when the task fails but the context remains alive.
        /// On context failure, the context also fails and is surfaced via the FailedContext event.
        /// Note that receiving an ActiveContext here is no guarantee that the context (and evaluator)
        /// are in a consistent state. Application developers need to investigate the reason available
        /// via getCause() to make that call.
        /// return the context the Task ran on.
        /// </summary>
        public Optional<IActiveContext> GetActiveContext()
        {
            IActiveContext activeContext = new ActiveContext(ActiveContextClr2Java);
            return ActiveContextClr2Java == null ? Optional<IActiveContext>.Empty() : Optional<IActiveContext>.Of(activeContext);
        }

        public Exception AsError()
        {
            throw new NotImplementedException();
        }

        private void Parse(IFailedTaskClr2Java failedTaskClr2Java)
        {
            string serializedInfo = failedTaskClr2Java.GetString();
            LOGGER.Log(Level.Verbose, "serialized failed task: " + serializedInfo);
            Dictionary<string, string> settings = new Dictionary<string, string>();
            string[] components = serializedInfo.Split(',');
            foreach (string component in components)
            {
                string[] pair = component.Trim().Split('=');
                if (pair == null || pair.Length != 2)
                {
                    Exceptions.Throw(new ArgumentException("invalid component to be used as key-value pair:", component), LOGGER);
                }
                settings.Add(pair[0], pair[1]);
            }

            string id;
            if (!settings.TryGetValue("Identifier", out id))
            {
                Exceptions.Throw(new ArgumentException("cannot find Identifier entry."), LOGGER);
            }
            Id = id;

            string msg;
            if (!settings.TryGetValue("Message", out msg))
            {
                LOGGER.Log(Level.Verbose, "no Message in Failed Task.");
                msg = string.Empty;
            }
            Message = msg;

            string description;
            if (!settings.TryGetValue("Description", out description))
            {
                LOGGER.Log(Level.Verbose, "no Description in Failed Task.");
                description = string.Empty;
            }
            Description = string.IsNullOrWhiteSpace(description) ? Optional<string>.Empty() : Optional<string>.Of(description);

            string cause;
            if (!settings.TryGetValue("Cause", out cause))
            {
                LOGGER.Log(Level.Verbose, "no Cause in Failed Task.");
                cause = string.Empty;
            }
            Reason = string.IsNullOrWhiteSpace(cause) ? Optional<string>.Empty() : Optional<string>.Of(cause);

            string rawData;
            if (!settings.TryGetValue("Data", out rawData))
            {
                LOGGER.Log(Level.Verbose, "no Data in Failed Task.");
                rawData = string.Empty;
            }
            Data = string.IsNullOrWhiteSpace(rawData) ? Optional<byte[]>.Empty() : Optional<byte[]>.Of(ByteUtilities.StringToByteArrays(rawData));
        }
    }
}
