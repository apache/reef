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
using System.Xml;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Utilities
{
    public static class Yarn
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(Yarn));

        public const string HadoopConfDirEnvVariable = "HADOOP_CONF_DIR";
        public const string YarnConfigFileName = "yarn-site.xml";

        private const string YarnRmWebappHttpsAddressPropertyName = "yarn.resourcemanager.webapp.https.address";
        private const string YarnRmWebappHttpAddressPropertyName = "yarn.resourcemanager.webapp.address";

        private const string RMIdsProperty = "yarn.resourcemanager.ha.rm-ids";

        public static IEnumerable<Uri> GetYarnRMWebappEndpoints(string hadoopConfigDir = null, bool useHttps = false)
        {
            hadoopConfigDir = string.IsNullOrWhiteSpace(hadoopConfigDir) ? 
                Environment.GetEnvironmentVariable(HadoopConfDirEnvVariable) 
                : hadoopConfigDir;

            if (string.IsNullOrEmpty(hadoopConfigDir) || !Directory.Exists(hadoopConfigDir))
            {
                throw new ArgumentException(HadoopConfDirEnvVariable + " is not configured or does not exist.",
                    "hadoopConfigDir");
            }

            Logger.Log(Level.Verbose, "Using {0} as hadoop configuration directory", hadoopConfigDir);
            var yarnConfigurationFile = Path.Combine(hadoopConfigDir, YarnConfigFileName);
            var doc = new XmlDocument();
            doc.Load(yarnConfigurationFile);

            var rmIdsText = GetValueNodeTextWithPropertyName(doc, RMIdsProperty);
            if (rmIdsText == null)
            {
                // No RM HA, only single RM.
                return GetRMWebappEndpointWithSingleRM(doc, useHttps);
            }

            var rmIds = rmIdsText.Split(',');
            var rmIdWebAppEndpoints = new List<Uri>();

            foreach (var rmId in rmIds)
            {
                var rmAddrPropertyToUse = useHttps ? YarnRmWebappHttpsAddressPropertyName : YarnRmWebappHttpAddressPropertyName;
                var rmWebAppAddressProperty = rmAddrPropertyToUse + "." + rmId;
                var rmWebAppAddressNodeText = GetValueNodeTextWithPropertyName(doc, rmWebAppAddressProperty);
                if (string.IsNullOrWhiteSpace(rmWebAppAddressNodeText))
                {
                    continue;
                }

                try
                {
                    rmIdWebAppEndpoints.Add(YarnRmWebAppUriFromString(rmWebAppAddressNodeText, useHttps));
                }
                catch (UriFormatException e)
                {
                    Exceptions.Caught(e, Level.Warning, "Unable to format " + rmWebAppAddressNodeText + " to URI", Logger);
                }
            }

            return rmIdWebAppEndpoints;
        }

        private static IEnumerable<Uri> GetRMWebappEndpointWithSingleRM(XmlNode doc, bool useHttps)
        {
            var rmAddressNodeText = GetValueNodeTextWithPropertyName(
                doc,
                useHttps ? YarnRmWebappHttpsAddressPropertyName : YarnRmWebappHttpAddressPropertyName);
            if (string.IsNullOrWhiteSpace(rmAddressNodeText))
            {
                throw new ApplicationException("Unable to find RM Webapp Address from yarn-site.xml.");
            }

            return new[] { YarnRmWebAppUriFromString(rmAddressNodeText, useHttps) };
        }

        private static string GetValueNodeTextWithPropertyName(XmlNode doc, string propertyName)
        {
            var node = doc.SelectSingleNode("/configuration/property[name='" + propertyName + "']/value/text()");
            return node == null ? null : node.Value;
        }

        private static Uri YarnRmWebAppUriFromString(string webAppUriStr, bool useHttps)
        {
            var protocolStr = useHttps ? "https://" : "http://";
            var text = webAppUriStr.TrimEnd('/') + "/";
            return new Uri(protocolStr + text);
        }
    }
}