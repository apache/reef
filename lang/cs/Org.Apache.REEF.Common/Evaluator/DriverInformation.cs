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
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using Org.Apache.REEF.Common.Avro;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Common.Evaluator
{
    public sealed class DriverInformation
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(DriverInformation));

        private readonly string _rid;

        private readonly string _startTime;

        private readonly string _nameServerId;

        private readonly IList<AvroReefServiceInfo> _services;

        public DriverInformation(string rid, string startTime, IList<AvroReefServiceInfo> services)
        {
            _rid = rid;
            _startTime = startTime;
            _services = services;

            if (_services == null)
            {
                LOGGER.Log(Level.Warning, "no services information from driver.");
            }
            else
            {
                AvroReefServiceInfo nameServerInfo =
                  _services.FirstOrDefault(
                      s => s.serviceName.Equals(Constants.NameServerServiceName, StringComparison.OrdinalIgnoreCase));
                if (nameServerInfo != null)
                {
                    _nameServerId = nameServerInfo.serviceInfo;
                }
            }
        }

        public string DriverRemoteIdentifier
        {
            get
            {
                return _rid;
            }
        }

        public string DriverStartTime
        {
            get
            {
                return _startTime;
            }
        }

        public string NameServerId
        {
            get
            {
                return _nameServerId;
            }
        }

        public static DriverInformation GetDriverInformationFromHttp(Uri queryUri)
        {
            while (queryUri != null)
            {
                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(queryUri);
                request.AllowAutoRedirect = true;
                request.KeepAlive = false;
                request.ContentType = "text/html";

                queryUri = null;
                try
                {
                    using (HttpWebResponse webResponse = (HttpWebResponse)request.GetResponse())
                    {
                        var refresh = webResponse.Headers.AllKeys.FirstOrDefault(k => k.Equals("refresh", StringComparison.OrdinalIgnoreCase));

                        if (refresh != null)
                        {
                            var refreshContent = webResponse.Headers.GetValues(refresh);
                            foreach (var refreshParam in refreshContent.SelectMany(content => content.Split(';').Select(c => c.Trim())))
                            {
                                var refreshKeyValue = refreshParam.Split('=').Select(kv => kv.Trim()).ToArray();
                                if (refreshKeyValue.Length == 2 && refreshKeyValue[0].Equals("url", StringComparison.OrdinalIgnoreCase))
                                {
                                    queryUri = new Uri(refreshKeyValue[1]);
                                    break;
                                }
                            }
                        }

                        // We have received a redirect URI, look there instead.
                        if (queryUri != null)
                        {
                            LOGGER.Log(Level.Verbose, "Received redirect URI:[{0}], redirecting...", queryUri);
                            continue;
                        }

                        Stream stream = webResponse.GetResponseStream();
                        if (stream == null || !stream.CanRead)
                        {
                            return null;
                        }

                        using (StreamReader streamReader = new StreamReader(stream, Encoding.UTF8))
                        {
                            var driverInformation = streamReader.ReadToEnd();
                            LOGGER.Log(Level.Verbose, "Http response line: {0}", driverInformation);
                            AvroDriverInfo info = AvroJsonSerializer<AvroDriverInfo>.FromString(driverInformation);
                            
                            if (info == null)
                            {
                                LOGGER.Log(Level.Info, "Cannot read content: {0}.", driverInformation);
                                return null;
                            }
                            
                            return new DriverInformation(info.remoteId, info.startTime, info.services);
                        }
                    }
                }
                catch (WebException)
                {
                    LOGGER.Log(Level.Warning, "In RECOVERY mode, cannot connect to [{0}] for driver information, will try again later.", queryUri);
                    return null;
                }
            }

            return null;
        }
    }
}