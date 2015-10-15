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

using System.Collections.Generic;
using Newtonsoft.Json;
using Org.Apache.REEF.Utilities.Logging;
using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;

namespace Org.Apache.REEF.Client.Common
{
    internal class HttpClientHelper : IDriverHttpEndpoint
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof (HttpClientHelper));
        private const int MaxConnectAttemptCount = 20;
        private const int MilliSecondsToWaitBeforeNextConnectAttempt = 1000;
        private const int SecondsForHttpClientTimeout = 120;
        private const string UnAssigned = "UNASSIGNED";
        private const string TrackingUrlKey = "trackingUrl";
        private const string AppKey = "app";
        private const string ThisIsStandbyRm = "This is standby RM";
        private const string AppJson = "application/json";

        private string _driverUrl;

        private readonly HttpClient _client;

        internal HttpClientHelper()
        {
            _client = new HttpClient
            {
                Timeout = TimeSpan.FromSeconds(SecondsForHttpClientTimeout),
            };
            _client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue(AppJson));
        }

        public string DriverUrl { get { return _driverUrl; } }

        public string GetUrlResult(string url)
        {
            var task = Task.Run(() => CallUrl(url));
            task.Wait();
            return task.Result;
        }

        enum UrlResultKind
        {
            WasNotAbleToTalkToRm,
            BackupRm,
            AppIdNotThereYet,
            UrlNotAssignedYet,
            GotAppIdUrl,
        }

        internal static List<string> GetRmUri(string filePath)
        {
            using (var sr = new StreamReader(File.Open(filePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
            {
                sr.ReadLine(); // appid 
                sr.ReadLine(); // trackingUrl
                var rmList = new List<string>();
                var rmUri = sr.ReadLine();
                while (rmUri != null)
                {                    
                    rmList.Add(rmUri);
                    rmUri = sr.ReadLine();
                }
                return rmList;
            }
        }

        internal static string GetAppId(string filePath)
        {
            using (var sr = new StreamReader(File.Open(filePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
            {
                var appId = sr.ReadLine();                
                return appId;
            }
        }

        internal static string GetTrackingUrl(string filePath)
        {
            using (var sr = new StreamReader(File.Open(filePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
            {
                sr.ReadLine(); // appid
                var trackingUrl = sr.ReadLine();
                return "http://" + trackingUrl + "/";
            }
        }

        internal async Task<string> CallUrl (string url)
        {
            var result = await TryGetUri(url);
            if (HasCommandFailed(result))
            {
                return null;
            }
            LOGGER.Log(Level.Warning, "CallUrl result " + result.Item2);
            return result.Item2;
        }

        internal string GetDriverUrlForYarn(String filePath)
        {
            _driverUrl = GetTrackingUrl(filePath);
            return _driverUrl;
        }

        internal string GetDriverUrlForLocalRuntime(string filePath)
        {
            _driverUrl = null;
            for (int i = 0; i < 10; i++)
            {
                var driverUrl = TryReadHttpServerIpAndPortFromFile(filePath);
                if (!string.IsNullOrEmpty(driverUrl))
                {
                    _driverUrl = "http://" + driverUrl + "/";
                    break;
                }
                Thread.Sleep(1000);
            }
            return _driverUrl;
        }

        private string TryReadHttpServerIpAndPortFromFile(String fileName)
        {
            string httpServerIpAndPort = null;
            try
            {
                LOGGER.Log(Level.Info, "try open " + fileName);
                using (var rdr = new StreamReader(File.OpenRead(fileName)))
                {
                    httpServerIpAndPort = rdr.ReadLine();
                    LOGGER.Log(Level.Info, "httpServerIpAndPort is " + httpServerIpAndPort);
                }
            }
            catch (FileNotFoundException)
            {
                LOGGER.Log(Level.Info, "File does not exist: " + fileName);
            }
            return httpServerIpAndPort;
        }

        internal async Task<string> GetAppIdTrackingUrl(string url)
        {
            var result = await TryGetUri(url);
            if (HasCommandFailed(result) ||  
                result.Item2 == null)                
            {
                return null;
            }

            LOGGER.Log(Level.Info, "GetAppIdTrackingUrl: " + result.Item2);
            return result.Item2;
        }

        private static bool ShouldRetry(HttpRequestException httpRequestException)
        {
            var shouldRetry = false;
            if (httpRequestException.Message.IndexOf(((int)(HttpStatusCode.NotFound)).ToString(), StringComparison.Ordinal) != -1 ||
                httpRequestException.Message.IndexOf(((int)(HttpStatusCode.BadGateway)).ToString(), StringComparison.Ordinal) != -1)
            {
                shouldRetry = true;
            }
            else
            {
                var webException = httpRequestException.InnerException as System.Net.WebException;
                if (webException != null)
                {
                    if (webException.Status == System.Net.WebExceptionStatus.ConnectFailure)
                    {
                        shouldRetry = true;
                    }
                }
            }
            return shouldRetry;
        }

        private static Tuple<bool, string> CommandFailed(String reason)
        {
            return new Tuple<bool, string>(false, null);
        }

        private static Tuple<bool, string> CommandSucceeded(string commandResult)
        {
            return new Tuple<bool, string>(true, commandResult);
        }

        private bool HasCommandFailed(Tuple<bool, string> httpCallResult)
        {
            return !httpCallResult.Item1;
        }

        internal async Task<Tuple<bool, string>> TryGetUri(string commandUri)
        {
            var connectAttemptCount = 0;
            Tuple<bool, string> result;

            while (true)
            {
                try
                {
                    string strResult = null;
                    LOGGER.Log(Level.Warning, "Try url [" + commandUri + "] connectAttemptCount " + connectAttemptCount + ".");
                    strResult = await _client.GetStringAsync(commandUri);
                    result = CommandSucceeded(strResult);
                    LOGGER.Log(Level.Warning, "Connection succeeded. connectAttemptCount was " + connectAttemptCount + ".");
                    break;
                }
                catch (HttpRequestException httpRequestException)
                {
                    if (!ShouldRetry(httpRequestException))
                    {
                        LOGGER.Log(Level.Error,
                            commandUri + " exception " + httpRequestException.Message + "\n" +
                            httpRequestException.StackTrace);
                        result = CommandFailed(httpRequestException.Message);
                        LOGGER.Log(Level.Warning, "Connection failed. connectAttemptCount was " + connectAttemptCount + ".");
                        break;
                    }
                }
                catch (Exception ex)
                {
                    LOGGER.Log(Level.Error, commandUri + " exception " + ex.Message + "\n" + ex.StackTrace);
                    result = CommandFailed(ex.Message);
                    LOGGER.Log(Level.Warning, "Connection failed. connectAttemptCount was " + connectAttemptCount + ".");
                    break;
                }

                ++connectAttemptCount;
                if (connectAttemptCount >= MaxConnectAttemptCount)
                {
                    result = CommandFailed("Could not connect to " + commandUri + " after " + MaxConnectAttemptCount.ToString() + "attempts.");
                    LOGGER.Log(Level.Warning, "Connection failed. connectAttemptCount was " + connectAttemptCount + ".");
                    break;
                }

                Thread.Sleep(MilliSecondsToWaitBeforeNextConnectAttempt);
            }

            return result;
        }

        internal async Task<string> TryUntilNoConnection(string commandUri)
        {
            var connectAttemptCount = 0;
            while (true)
            {
                try
                {
                    var strResult = await _client.GetStringAsync(commandUri);
                    LOGGER.Log(Level.Info,
                        "Connection succeeded. connectAttemptCount was " + connectAttemptCount + ".");
                }
                catch (HttpRequestException httpRequestException)
                {
                    LOGGER.Log(Level.Info, httpRequestException.Message);
                    break;
                }
                catch (Exception e)
                {
                    LOGGER.Log(Level.Info, e.Message);
                    break;
                }

                ++connectAttemptCount;
                if (connectAttemptCount >= MaxConnectAttemptCount)
                {
                    LOGGER.Log(Level.Info, "Can still connect to " + commandUri + " after " + MaxConnectAttemptCount.ToString() + "attempts.");
                    break;
                }

                Thread.Sleep(MilliSecondsToWaitBeforeNextConnectAttempt);
            }

            return null;
        }

        private static bool ShouldRetry(HttpStatusCode httpStatusCode)
        {
            return httpStatusCode == HttpStatusCode.NotFound;
        }

        private UrlResultKind CheckUrlAttempt(string result)
        {
            UrlResultKind resultKind = UrlResultKind.WasNotAbleToTalkToRm;
            if (string.IsNullOrEmpty(result))
            {
                resultKind = UrlResultKind.WasNotAbleToTalkToRm;
            }
            else if (result.StartsWith(ThisIsStandbyRm))
            {
                resultKind = UrlResultKind.BackupRm;
            }
            else
            {
                dynamic deserializedValue = JsonConvert.DeserializeObject(result);
                var values = deserializedValue[AppKey];
                if (values == null || values[TrackingUrlKey] == null)
                {
                    resultKind = UrlResultKind.AppIdNotThereYet;
                }
                else
                {
                    _driverUrl = values[TrackingUrlKey].ToString();
                    LOGGER.Log(Level.Info, "trackingUrl[" + _driverUrl + "]");

                    if (0 == String.Compare(_driverUrl, UnAssigned))
                    {
                        resultKind = UrlResultKind.UrlNotAssignedYet;
                    }
                    else
                    {
                        resultKind = UrlResultKind.GotAppIdUrl;
                    }

                }
            }

            LOGGER.Log(Level.Info, "CheckUrlAttempt " + resultKind);
            return resultKind;
        }
    }
}
