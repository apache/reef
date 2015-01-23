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

using Org.Apache.Reef.Common.Avro;
using Org.Apache.Reef.Utilities.Logging;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;

namespace Org.Apache.Reef.Common.Evaluator
{
    public class DriverInformation
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(DriverInformation));
        
        private string _rid;

        private string _startTime;

        private string _nameServerId;

        private IList<AvroReefServiceInfo> _services;

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
            HttpWebRequest request = (HttpWebRequest)WebRequest.Create(queryUri);
            request.AllowAutoRedirect = false;
            request.KeepAlive = false;
            request.ContentType = "text/html";

            string driverInfomation;
            AvroDriverInfo info = null;
            try
            {
                using (HttpWebResponse webResponse = (HttpWebResponse)request.GetResponse())
                {
                    Stream stream = webResponse.GetResponseStream();
                    if (stream == null)
                    {
                        return null;
                    }
                    using (StreamReader streamReader = new StreamReader(stream, Encoding.UTF8))
                    {
                        driverInfomation = streamReader.ReadToEnd();
                        LOGGER.Log(Level.Verbose, "Http response line: " + driverInfomation);
                        info = AvroJsonSerializer<AvroDriverInfo>.FromString(driverInfomation);
                    }
                }
            }
            catch (WebException)
            {
                LOGGER.Log(Level.Warning, string.Format(CultureInfo.InvariantCulture, "In RECOVERY mode, cannot connect to [{0}] for driver information, will try again later.", queryUri));
                return null;
            }
            catch (Exception e)
            {
                Org.Apache.Reef.Utilities.Diagnostics.Exceptions.CaughtAndThrow(e, Level.Error, string.Format(CultureInfo.InvariantCulture, "Cannot read content from {0}.", queryUri), LOGGER);
            }

            if (info != null)
            {
                LOGGER.Log(
                    Level.Verbose, 
                    string.Format(CultureInfo.InvariantCulture, "Driver information extracted with remote identier [{0}], start time [{1}], and servics [{2}]", info.remoteId, info.startTime, info.services));
                return new DriverInformation(info.remoteId, info.startTime, info.services);
            }
            return null;
        }
    }
}
