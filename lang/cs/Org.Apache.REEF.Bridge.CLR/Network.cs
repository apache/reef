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
using System.Collections.Concurrent;
using System.IO;
using System.Net;
using System.Threading;
using org.apache.reef.bridge.message;
using Org.Apache.REEF.Common.Files;
using Org.Apache.REEF.IO.FileSystem;
using Org.Apache.REEF.IO.FileSystem.Local;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Wake.Remote.Impl;

namespace Org.Apache.REEF.Bridge
{
    /// <summary>
    /// Avro message protocol network to send messages to the java bridge.
    /// </summary>
    public sealed class Network
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(Network));
        private static long identifierSource = 0;
        private IRemoteManager<byte[]> remoteManager;
        private IObserver<byte[]> remoteObserver;
        private LocalObserver localObserver;
        private BlockingCollection<byte[]> queue = new BlockingCollection<byte[]>();

        /// <summary>
        /// Construct a network stack using the wate remote manager.
        /// </summary>
        /// <param name="localAddressProvider">An address provider used to obtain a local IP address on an open port.</param>
        /// <param name="messageObserver">Message receiver that implements IObservable for each message to be processed</param>
        public Network(ILocalAddressProvider localAddressProvider, object messageObserver)
        {
            this.localObserver = new LocalObserver(messageObserver);

            // Instantiate a file system proxy.
            IFileSystem fileSystem = TangFactory.GetTang()
                .NewInjector(LocalFileSystemConfiguration.ConfigurationModule.Build())
                .GetInstance<IFileSystem>();

            // Get the path to the bridge name server endpoint file.
            string javaBridgeAddress = null;
            REEFFileNames fileNames = new REEFFileNames();
            using (FileStream stream = File.Open(fileNames.GetDriverJavaBridgeEndpoint(), FileMode.Open))
            {
                using (StreamReader reader = new StreamReader(stream))
                {
                    javaBridgeAddress = reader.ReadToEnd();
                }
            }
            Logger.Log(Level.Info, string.Format("Name Server Address: {0}", (javaBridgeAddress == null) ? "NULL" : javaBridgeAddress));

            BuildRemoteManager(localAddressProvider, javaBridgeAddress);
        }

        /// <summary>
        /// Send a message to the java side of the bridge.
        /// </summary>
        /// <param name="message">An object reference to a message in the org.apache.reef.bridge.message package.</param>
        /// <returns></returns>
        public long send(object message)
        {
            long identifier = Interlocked.Increment(ref identifierSource);
            remoteObserver.OnNext(Serializer.Write(message, identifier));
            return identifier;
        }

        /// <summary>
        /// Processes messages received by the remote manager.
        /// </summary>
        private class LocalObserver : IObserver<IRemoteMessage<byte[]>>
        {
            private object messageObserver;

            public LocalObserver(object messageObserver)
            {
                this.messageObserver = messageObserver;
            }

            /// <summary>
            /// Called by the remote manager to process messages received from the java bridge.
            /// </summary>
            /// <param name="message"></param>
            public void OnNext(IRemoteMessage<byte[]> message)
            {
                Logger.Log(Level.Info, "Message received: " + message.Identifier.ToString());

                // Deserialize the message and invoke the appropriate handler.
                Serializer.Read(message.Message, messageObserver);
            }

            public void OnError(Exception error)
            {
                Logger.Log(Level.Info, "Error: [" + error.Message + "]");
            }

            public void OnCompleted()
            {
                Logger.Log(Level.Info, "Completed");
            }
        }

        /// <summary>
        /// Construct a remote manager to implement the low level network transport.
        /// </summary>
        /// <param name="localAddressProvider">An address provider used to obtain a local IP address on an open port.</param>
        /// <param name="javaBridgeAddress">A string which contains the IP and port of the java bridge.</param>
        private void BuildRemoteManager(
            ILocalAddressProvider localAddressProvider,
            string javaBridgeAddress)
        {
            // Instantiate the remote manager.
            IRemoteManagerFactory remoteManagerFactory =
                TangFactory.GetTang().NewInjector().GetInstance<IRemoteManagerFactory>();
            remoteManager = remoteManagerFactory.GetInstance(localAddressProvider.LocalAddress, new ByteCodec());

            // Listen to the java bridge on the local end point.
            remoteManager.RegisterObserver(localObserver);
            Logger.Log(Level.Info, string.Format("Local observer listening to java bridge on: [{0}]", remoteManager.LocalEndpoint.ToString()));

            // Instantiate a remote observer to send messages to the java bridge.
            string[] javaAddressStrs = javaBridgeAddress.Split(':');
            IPAddress javaBridgeIpAddress = IPAddress.Parse(javaAddressStrs[0]);
            int port = int.Parse(javaAddressStrs[1]);
            IPEndPoint javaIpEndPoint = new IPEndPoint(javaBridgeIpAddress, port);
            remoteObserver = remoteManager.GetRemoteObserver(javaIpEndPoint);
            Logger.Log(Level.Info, string.Format("Connecting to java bridge on: [{0}]", javaIpEndPoint.ToString()));

            // Negotiate the protocol.
            Serializer.Initialize();
            send(new Protocol(100));
        }
    }
}
