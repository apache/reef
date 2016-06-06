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
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Utilities.Runtime.Yarn
{
    [Unstable("0.16. Namespace may move.")]
    public sealed class YarnConfiguration
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(YarnConfiguration));

        /// <summary>
        /// The Hadoop configuration directory environment variable.
        /// </summary>
        public static readonly string HadoopConfDirEnvVariable = "HADOOP_CONF_DIR";

        /// <summary>
        /// The YARN configuration XML file name.
        /// </summary>
        public static readonly string YarnConfigFileName = "yarn-site.xml";

        /// <summary>
        /// The RM Web application address configuration property.
        /// </summary>
        public static readonly string RMWebappHttpsAddress = "yarn.resourcemanager.webapp.https.address";

        /// <summary>
        /// The RM Web application address configuration property.
        /// </summary>
        public static readonly string RMWebappAddress = "yarn.resourcemanager.webapp.address";

        public static readonly string YARNHttpPolicyKey = "yarn.http.policy";

        public static readonly string RMHaEnabled = "yarn.resourcemanager.ha.enabled";

        /// <summary>
        /// The RM HA IDs property.
        /// </summary>
        private const string RMHaIds = "yarn.resourcemanager.ha.rm-ids";

        private readonly XmlDocument _yarnSiteXmlDoc;
        private readonly bool _useHttps;

        /// <summary>
        /// Returns a configuration representing the YARN configuration on the cluster.
        /// </summary>
        /// <param name="hadoopConfDir">
        /// The hadoop configuration directory, defaults to use the environment variable <see cref="HadoopConfDirEnvVariable"/>.
        /// </param>
        /// <param name="yarnConfigFileName">
        /// The YARN configuration file name, defaults to <see cref="YarnConfigFileName"/>
        /// </param>
        /// <param name="useHttps">
        /// Whether or not to use HTTPS, defaults to read from YARN configuration.
        /// </param>
        public static YarnConfiguration GetConfiguration(
            string hadoopConfDir = null,
            string yarnConfigFileName = null,
            bool? useHttps = null)
        {
            return new YarnConfiguration(hadoopConfDir, yarnConfigFileName, useHttps);
        }

        private YarnConfiguration(
            string hadoopConfDir,
            string yarnConfigFileName,
            bool? useHttps)
        {
            var hadoopConfigDir = string.IsNullOrWhiteSpace(hadoopConfDir) ?
                Environment.GetEnvironmentVariable(HadoopConfDirEnvVariable)
                : hadoopConfDir;

            yarnConfigFileName = string.IsNullOrWhiteSpace(yarnConfigFileName) ? 
                YarnConfigFileName 
                : yarnConfigFileName;

            if (string.IsNullOrEmpty(hadoopConfigDir) || !Directory.Exists(hadoopConfigDir))
            {
                throw new ArgumentException(hadoopConfigDir + " is not configured or does not exist.");
            }

            Logger.Log(Level.Verbose, "Using {0} as hadoop configuration directory", hadoopConfigDir);
            var yarnConfigurationFile = Path.Combine(hadoopConfigDir, yarnConfigFileName);

            _yarnSiteXmlDoc = new XmlDocument();
            _yarnSiteXmlDoc.Load(yarnConfigurationFile);
            if (useHttps == null)
            {
                var httpPolicyStr = GetString(YARNHttpPolicyKey, HttpConfig.HttpOnlyPolicy);
                _useHttps = !httpPolicyStr.Equals(HttpConfig.HttpOnlyPolicy);
            }
            else
            {
                if (useHttps.Value)
                {
                    var httpPolicyStr = GetString(YARNHttpPolicyKey, HttpConfig.HttpOnlyPolicy);
                    if (httpPolicyStr.Equals(HttpConfig.HttpOnlyPolicy))
                    {
                        throw new ArgumentException("YARN cluster does not support HTTPS when useHttps is set to true.");
                    }
                }

                _useHttps = useHttps.Value;
            }
        }

        /// <summary>
        /// Gets the YARN RM web application endpoints.
        /// </summary>
        public IEnumerable<Uri> GetYarnRMWebappEndpoints()
        {
            if (GetBool(RMHaEnabled))
            {
                var rmIds = GetStrings(RMHaIds);
                if (rmIds == null || rmIds.Length == 0)
                {
                    throw new ApplicationException("RM HA enabled, but RM IDs were not found.");
                }

                var rmIdWebAppEndpoints = new List<Uri>();

                foreach (var rmId in rmIds)
                {
                    var rmAddrPropertyToUse = _useHttps ? RMWebappHttpsAddress : RMWebappAddress;
                    var rmWebAppAddressProperty = rmAddrPropertyToUse + "." + rmId;
                    bool isFound;
                    var rmWebAppAddressNodeText = GetString(rmWebAppAddressProperty, out isFound);
                    if (!isFound)
                    {
                        continue;
                    }

                    try
                    {
                        rmIdWebAppEndpoints.Add(UriFromString(rmWebAppAddressNodeText));
                    }
                    catch (UriFormatException e)
                    {
                        Exceptions.Caught(e,
                            Level.Warning,
                            "Unable to format " + rmWebAppAddressNodeText + " to URI",
                            Logger);
                    }
                }

                if (rmIdWebAppEndpoints.Count == 0)
                {
                    throw new ApplicationException("RM HA enabled, but RM IDs were not found.");
                }

                return rmIdWebAppEndpoints;
            }

            var rmAddressNodeText = GetString(_useHttps ? RMWebappHttpsAddress : RMWebappAddress);
            if (string.IsNullOrWhiteSpace(rmAddressNodeText))
            {
                throw new ApplicationException("Unable to find RM Webapp Address from yarn-site.xml.");
            }

            return new[] { UriFromString(rmAddressNodeText) };
        }

        /// <summary>
        /// Gets the bool value of an XML node under
        /// /configuration/property with name <see cref="propertyName"/> in the YARN configuration file.
        /// </summary>
        /// <remarks>
        /// If does not exist or is not boolean, returns defaultValue.
        /// </remarks>
        public bool GetBool(string propertyName, bool defaultValue = false)
        {
            bool isFound;
            var str = GetString(propertyName, out isFound);

            bool value;
            if (isFound && bool.TryParse(str, out value))
            {
                return value;
            }

            return defaultValue;
        }

        /// <summary>
        /// Gets the text value of an XML node under 
        /// /configuration/property with name <see cref="propertyName"/> in the YARN configuration file.
        /// </summary>
        public string GetString(string propertyName, string defaultValue = null)
        {
            bool isFound;
            var str = GetString(propertyName, out isFound);
            return isFound ? str : defaultValue;
        }

        /// <summary>
        /// Gets the comma delimited text values of an XML node under 
        /// /configuration/property with name <see cref="propertyName"/> in the YARN configuration file.
        /// </summary>
        public string[] GetStrings(string propertyName, string[] defaultValues = null)
        {
            bool isFound;
            var propertyStr = GetString(propertyName, out isFound);
            return isFound ? propertyStr.Split(',') : defaultValues;
        }

        /// <summary>
        /// Gets the text value of an XML node under 
        /// /configuration/property with name <see cref="propertyName"/> in the YARN configuration file.
        /// </summary>
        private string GetString(string propertyName, out bool isFound)
        {
            var node = _yarnSiteXmlDoc
                .SelectSingleNode("/configuration/property[name='" + propertyName + "']/value/text()");
            isFound = node != null;
            return node == null ? null : node.Value;
        }

        /// <summary>
        /// Returns a URI from string.
        /// </summary>
        private Uri UriFromString(string webAppUriStr)
        {
            var protocolStr = _useHttps ? "https://" : "http://";
            var text = webAppUriStr.TrimEnd('/') + "/";
            return new Uri(protocolStr + text);
        }
    }
}