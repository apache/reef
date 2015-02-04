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

using System.Net;
using Org.Apache.Reef.Driver.Bridge;
using Org.Apache.Reef.Utilities;
using Org.Apache.Reef.Utilities.Logging;
using Org.Apache.Reef.Tang.Annotations;

namespace Org.Apache.Reef.Driver.Defaults
{
    public class DefaultHttpHandler : IHttpHandler
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(DefaultHttpHandler));

        [Inject]
        public DefaultHttpHandler()
        {
        }

        public string GetSpecification()
        {
            return "Ping";
        }

        public void OnHttpRequest(ReefHttpRequest requet, ReefHttpResponse response) 
        {
            LOGGER.Log(Level.Info, "OnHttpRequest in DefaultHttpHandler is called.");
            response.Status = HttpStatusCode.OK;
            response.OutputStream = ByteUtilities.StringToByteArrays("Byte array returned from DefaultHttpHandler in CLR!!!");
        }
    }
}
