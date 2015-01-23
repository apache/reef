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

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;
using Org.Apache.Reef.Driver;
using Org.Apache.Reef.Driver.bridge;
using Org.Apache.Reef.Utilities;
using Org.Apache.Reef.Utilities.Logging;
using Org.Apache.Reef.Tang.Interface;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Timer = System.Timers.Timer;

namespace Org.Apache.Reef.Test
{
    public class ReefFunctionalTest
    {
        protected const string _stdout = "driver.stdout";
        protected const string _stderr = "driver.stderr";
        protected const string _cmdFile = "run.cmd";
        protected const string _binFolder = "bin";
        // TODO: we will need a proper way to hide this when we are OSS'ed
        protected const string _blobStorageConnectionString =
            @"DefaultEndpointsProtocol=https;AccountName=reeftestlog;AccountKey=cuUmPRF9DiG56bciNc37O/SfHAoh1jFfJW6AsXAtWLPnjlOzXJGWgXhkyDFOGEHIMscqDU41ElUKnjcsJjWD9w==";

        private bool _testSuccess = false;
        private bool _onLocalRuntime = false;
        private string _className = Constants.BridgeLaunchClass;
        private string _clrFolder = ".";
        private string _reefJar = Path.Combine(_binFolder, Constants.BridgeJarFileName);
        private string _runCommand = Path.Combine(_binFolder, _cmdFile);

        // TODO: once things stablize, we would like to toggle this to be false and only enable when needed for debugging test failures.
        private bool _enableRealtimeLogUpload = true; 

        protected string TestId { get; set; }

        protected Timer TestTimer { get; set; }

        protected Task TimerTask { get; set; }

        protected bool TestSuccess 
        {
            get { return _testSuccess; }
            set { _testSuccess = value; }
        }

        protected bool IsOnLocalRuntiime
        {
            get { return _onLocalRuntime; }
            set { _onLocalRuntime = value; }
        }

        public void Init()
        {
            TestId = Guid.NewGuid().ToString("N").Substring(0, 8);
            Console.WriteLine("Running test " + TestId + ". If failed AND log uploaded is enabled, log can be find in " + Path.Combine(DateTime.Now.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture), TestId));
            if (_enableRealtimeLogUpload)
            {
                TimerTask = new Task(() =>
                {
                    TestTimer = new Timer()
                    {
                        Interval = 1000,
                        Enabled = true,
                        AutoReset = true
                    };
                    TestTimer.Elapsed += PeriodicUploadLog;
                    TestTimer.Start();
                });
                TimerTask.Start(); 
            }
            
            ValidationUtilities.ValidateEnvVariable("JAVA_HOME");

            if (!Directory.Exists(_binFolder))
            {
                throw new InvalidOperationException(_binFolder + " not found in current directory, cannot init test");
            }
        }

        protected void TestRun(HashSet<string> appDlls, IConfiguration driverBridgeConfig, bool runOnYarn = false, JavaLoggingSetting javaLogSettings = JavaLoggingSetting.INFO)
        {
            ClrClientHelper.Run(appDlls, driverBridgeConfig, new DriverSubmissionSettings() { RunOnYarn = runOnYarn, JavaLogLevel = javaLogSettings }, _reefJar, _runCommand, _clrFolder, _className);
        }

        protected void CleanUp()
        {
            Console.WriteLine("Cleaning up test.");

            if (TimerTask != null)
            {
                TestTimer.Stop();
                TimerTask.Dispose();
                TimerTask = null;
            }
            
            // Wait for file upload task to complete
            Thread.Sleep(500);

            string dir = Path.Combine(Directory.GetCurrentDirectory(), "REEF_LOCAL_RUNTIME");
            try
            {
                if (Directory.Exists(dir))
                {
                    Directory.Delete(dir, true);
                }
            }
            catch (IOException)
            {
                // do not fail if clean up is unsuccessful
            }   
        }

        protected void ValidateSuccessForLocalRuntime(int numberOfEvaluatorsToClose)
        {
            const string successIndication = "EXIT: ActiveContextClr2Java::Close";
            const string failedTaskIndication = "Java_com_microsoft_reef_javabridge_NativeInterop_ClrSystemFailedTaskHandlerOnNext";
            const string failedEvaluatorIndication = "Java_com_microsoft_reef_javabridge_NativeInterop_ClrSystemFailedEvaluatorHandlerOnNext";
            string[] lines = File.ReadAllLines(GetLogFile(_stdout));
            string[] successIndicators = lines.Where(s => s.Contains(successIndication)).ToArray();
            string[] failedTaskIndicators = lines.Where(s => s.Contains(failedTaskIndication)).ToArray();
            string[] failedIndicators = lines.Where(s => s.Contains(failedEvaluatorIndication)).ToArray();
            Assert.IsTrue(successIndicators.Count() == numberOfEvaluatorsToClose);
            Assert.IsFalse(failedTaskIndicators.Any());
            Assert.IsFalse(failedIndicators.Any());
        }

        protected void PeriodicUploadLog(object source, ElapsedEventArgs e)
        {
            try
            {
                UploadDriverLog();
            }
            catch (Exception)
            {
                // log not available yet, ignore it
            }
        }

        protected string GetLogFile(string logFileName)
        {
            string driverContainerDirectory = Directory.GetDirectories(Path.Combine(Directory.GetCurrentDirectory(), "REEF_LOCAL_RUNTIME"), "driver", SearchOption.AllDirectories).SingleOrDefault();
            if (string.IsNullOrWhiteSpace(driverContainerDirectory))
            {
                throw new InvalidOperationException("Cannot find driver container directory");
            }
            string logFile = Path.Combine(driverContainerDirectory, logFileName);
            if (!File.Exists(logFile))
            {
                throw new InvalidOperationException("Driver stdout file not found");
            }
            return logFile;
        }

        private void UploadDriverLog()
        {
            string driverStdout = GetLogFile(_stdout);
            string driverStderr = GetLogFile(_stderr);
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(_blobStorageConnectionString);
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            CloudBlobContainer container = blobClient.GetContainerReference(DateTime.Now.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture));   
            container.CreateIfNotExists();

            CloudBlockBlob blob = container.GetBlockBlobReference(Path.Combine(TestId, "driverStdOut"));
            FileStream fs = new FileStream(driverStdout, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            blob.UploadFromStream(fs);
            fs.Close();

            blob = container.GetBlockBlobReference(Path.Combine(TestId, "driverStdErr"));
            fs = new FileStream(driverStderr, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            blob.UploadFromStream(fs);
            fs.Close();
        }
    }
}