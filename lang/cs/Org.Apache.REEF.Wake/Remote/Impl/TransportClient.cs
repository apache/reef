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
using System.Threading;
using System.Threading.Tasks;

namespace Org.Apache.REEF.Wake.Remote.Impl
{
    /// <summary>
    /// Establish connections to TransportServer for remote message passing
    /// </summary>
    public class TransportClient<T> : IDisposable
    {
        private readonly ILink<T> _link;
        private readonly IObserver<TransportEvent<T>> _observer;
        private readonly CancellationTokenSource _cancellationSource;
        private bool _disposed;

        /// <summary>
        /// Construct a TransportClient.
        /// Used to send messages to the specified remote endpoint.
        /// </summary>
        /// <param name="remoteEndpoint">The endpoint of the remote server to connect to</param>
        /// <param name="codec">Codec to decode/encode</param>
        /// <param name="clientFactory">TcpClient factory</param>
        public TransportClient(IPEndPoint remoteEndpoint, ICodec<T> codec, ITcpClientConnectionFactory clientFactory) 
        {
            if (remoteEndpoint == null)
            {
                throw new ArgumentNullException("remoteEndpoint");
            }
            if (codec == null)
            {
                throw new ArgumentNullException("codec");
            }
            if (clientFactory == null)
            {
                throw new ArgumentNullException("clientFactory");
            }

            _link = new Link<T>(remoteEndpoint, codec, clientFactory);
            _cancellationSource = new CancellationTokenSource();
            _disposed = false;
        }

        /// <summary>
        /// Construct a TransportClient.
        /// Used to send messages to the specified remote endpoint.
        /// </summary>
        /// <param name="remoteEndpoint">The endpoint of the remote server to connect to</param>
        /// <param name="codec">Codec to decode/encodec</param>
        /// <param name="observer">Callback used when receiving responses from remote host</param>
        /// <param name="clientFactory">TcpClient factory</param>
        public TransportClient(IPEndPoint remoteEndpoint,
            ICodec<T> codec,
            IObserver<TransportEvent<T>> observer,
            ITcpClientConnectionFactory clientFactory)
            : this(remoteEndpoint, codec, clientFactory)
        {
            _observer = observer;
            Task.Run(() => ResponseLoop());
        }

        /// <summary>
        /// Gets the underlying transport link.
        /// </summary>
        public ILink<T> Link
        {
            get { return _link; }
        }

        /// <summary>
        /// Send the remote message.
        /// </summary>
        /// <param name="message">The message to send</param>
        public void Send(T message)
        {
            if (message == null)
            {
                throw new ArgumentNullException("message");    
            }

            _link.Write(message);
        }

        /// <summary>
        /// Close all opened connections
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected void Dispose(bool disposing)
        {
            if (!_disposed && disposing)
            {
                _link.Dispose();
                _disposed = true;
            }
        }

        /// <summary>
        /// Continually read responses from remote host
        /// </summary>
        private async Task ResponseLoop()
        {
            while (!_cancellationSource.IsCancellationRequested)
            {
                T message = await _link.ReadAsync(_cancellationSource.Token);
                if (message == null)
                {
                    break;
                }

                TransportEvent<T> transportEvent = new TransportEvent<T>(message, _link);
                _observer.OnNext(transportEvent);
            }
        }
    }
}
