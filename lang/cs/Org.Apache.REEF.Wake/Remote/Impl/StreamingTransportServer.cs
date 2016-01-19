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
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.StreamingCodec;
using Org.Apache.REEF.Wake.Util;

namespace Org.Apache.REEF.Wake.Remote.Impl
{
    /// <summary>
    /// Server to handle incoming remote messages.
    /// </summary>
    /// <typeparam name="T">Generic Type of message. It is constrained to have implemented IWritable and IType interface</typeparam>
    internal sealed class StreamingTransportServer<T> : IDisposable
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(StreamingTransportServer<>));

        private TcpListener _listener;
        private readonly CancellationTokenSource _cancellationSource;
        private readonly IObserver<TransportEvent<T>> _remoteObserver;
        private readonly ITcpPortProvider _tcpPortProvider;
        private readonly IStreamingCodec<T> _streamingCodec;
        private bool _disposed;
        private Task _serverTask;

        /// <summary>
        /// Constructs a TransportServer to listen for remote events.  
        /// Listens on the specified remote endpoint.  When it receives a remote
        /// event, it will invoke the specified remote handler.
        /// </summary>
        /// <param name="address">Endpoint address to listen on</param>
        /// <param name="remoteHandler">The handler to invoke when receiving incoming
        /// remote messages</param>
        /// <param name="tcpPortProvider">Find port numbers if listenport is 0</param>
        /// <param name="streamingCodec">Streaming codec</param>
        internal StreamingTransportServer(
            IPAddress address,
            IObserver<TransportEvent<T>> remoteHandler,
            ITcpPortProvider tcpPortProvider,
            IStreamingCodec<T> streamingCodec)
        {
            _listener = new TcpListener(address, 0);
            _remoteObserver = remoteHandler;
            _tcpPortProvider = tcpPortProvider;
            _cancellationSource = new CancellationTokenSource();
            _cancellationSource.Token.ThrowIfCancellationRequested();
            _streamingCodec = streamingCodec;
            _disposed = false;
        }

        /// <summary>
        /// Returns the listening endpoint for the TransportServer 
        /// </summary>
        public IPEndPoint LocalEndpoint
        {
            get { return _listener.LocalEndpoint as IPEndPoint; }
        }

        /// <summary>
        /// Starts listening for incoming remote messages.
        /// </summary>
        public void Run()
        {
            FindAPortAndStartListener();
            _serverTask = Task.Run(() => StartServer());
        }

        private void FindAPortAndStartListener()
        {
            var foundAPort = false;
            var exception = new SocketException((int)SocketError.AddressAlreadyInUse);
            for (var enumerator = _tcpPortProvider.GetEnumerator();
                !foundAPort && enumerator.MoveNext();)
            {
                _listener = new TcpListener(LocalEndpoint.Address, enumerator.Current);
                try
                {
                    _listener.Start();
                    foundAPort = true;
                }
                catch (SocketException e)
                {
                    exception = e;
                }
            }
            if (!foundAPort)
            {
                Exceptions.Throw(exception, "Could not find a port to listen on", LOGGER);
            }
            LOGGER.Log(Level.Info,
                string.Format("Listening on {0}", _listener.LocalEndpoint.ToString()));
        }

        /// <summary>
        /// Close the TransportServer and all open connections
        /// </summary>
        public void Dispose()
        {
            if (!_disposed)
            {
                _cancellationSource.Cancel();

                try
                {
                    _listener.Stop();
                }
                catch (SocketException)
                {
                    LOGGER.Log(Level.Info, "Disposing of transport server before listener is created.");
                }

                if (_serverTask != null)
                {
                    _serverTask.Wait();

                    // Give the TransportServer Task 500ms to shut down, ignore any timeout errors
                    try
                    {
                        CancellationTokenSource serverDisposeTimeout = new CancellationTokenSource(500);
                        _serverTask.Wait(serverDisposeTimeout.Token);
                    }
                    catch (Exception e)
                    {
                        Console.Error.WriteLine(e);
                    }
                    finally
                    {
                        _serverTask.Dispose();
                    }
                }
            }

            _disposed = true;
        }

        /// <summary>
        /// Helper method to start TransportServer.  This will
        /// be run in an asynchronous Task.
        /// </summary>
        /// <returns>An asynchronous Task for the running server.</returns>
        private async Task StartServer()
        {
            try
            {
                while (!_cancellationSource.Token.IsCancellationRequested)
                {
                    TcpClient client = await _listener.AcceptTcpClientAsync().ConfigureAwait(false);
                    ProcessClient(client).Forget();
                }
            }
            catch (InvalidOperationException)
            {
                LOGGER.Log(Level.Info, "StreamingTransportServer has been closed.");
            }
            catch (OperationCanceledException)
            {
                LOGGER.Log(Level.Info, "StreamingTransportServer has been closed.");
            }
        }

        /// <summary>
        /// Receives event from connected TcpClient and invokes handler on the event.
        /// </summary>
        /// <param name="client">The connected client</param>
        private async Task ProcessClient(TcpClient client)
        {
            // Keep reading messages from client until they disconnect or timeout
            CancellationToken token = _cancellationSource.Token;
            using (ILink<T> link = new StreamingLink<T>(client, _streamingCodec))
            {
                while (!token.IsCancellationRequested)
                {
                    T message = await link.ReadAsync(token);

                    if (message == null)
                    {
                        break;
                    }

                    TransportEvent<T> transportEvent = new TransportEvent<T>(message, link);
                    _remoteObserver.OnNext(transportEvent);
                }
                LOGGER.Log(Level.Error,
                    "ProcessClient close the Link. IsCancellationRequested: " + token.IsCancellationRequested);
            }
        }
    }
}