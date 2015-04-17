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
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Util;

namespace Org.Apache.REEF.Wake.Remote.Impl
{
    /// <summary>
    /// Represents an open connection between remote hosts. This class is not thread safe
    /// </summary>
    /// <typeparam name="T">Generic Type of message. It is constrained to have implemented IWritable and IType interface</typeparam>
    [Obsolete("Need to remove Iwritable and use IstreamingCodec. Please see Jira REEF-295 ", false)]
    public class WritableLink<T> : ILink<T> where T : IWritable
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof (WritableLink<T>));

        private readonly IPEndPoint _localEndpoint;
        private bool _disposed;
        private readonly NetworkStream _stream;
        
        /// <summary>
        /// Cache structure to store the constructor functions for various types.
        /// </summary>
        private readonly TypeCache<T> _cache;

        /// <summary>
        /// Stream reader to be passed to IWritable
        /// </summary>
        private readonly StreamDataReader _reader;

        /// <summary>
        /// Stream writer from which to read from IWritable
        /// </summary>
        private readonly StreamDataWriter _writer;

        /// <summary>
        /// Constructs a Link object.
        /// Connects to the specified remote endpoint.
        /// </summary>
        /// <param name="remoteEndpoint">The remote endpoint to connect to</param>
        public WritableLink(IPEndPoint remoteEndpoint)
        {
            if (remoteEndpoint == null)
            {
                throw new ArgumentNullException("remoteEndpoint");
            }

            Client = new TcpClient();
            Client.Connect(remoteEndpoint);

            _stream = Client.GetStream();
            _localEndpoint = GetLocalEndpoint();
            _disposed = false;
            _cache = new TypeCache<T>();
            _reader = new StreamDataReader(_stream);
            _writer = new StreamDataWriter(_stream);
        }

        /// <summary>
        /// Constructs a Link object.
        /// Uses the already connected TcpClient.
        /// </summary>
        /// <param name="client">The already connected client</param>
        public WritableLink(TcpClient client)
        {
            if (client == null)
            {
                throw new ArgumentNullException("client");
            }

            Client = client;
            _stream = Client.GetStream();
            _localEndpoint = GetLocalEndpoint();
            _disposed = false;
            _cache = new TypeCache<T>();
            _reader = new StreamDataReader(_stream);
            _writer = new StreamDataWriter(_stream);
        }

        /// <summary>
        /// Returns the local socket address
        /// </summary>
        public IPEndPoint LocalEndpoint
        {
            get { return _localEndpoint; }
        }

        /// <summary>
        /// Returns the remote socket address
        /// </summary>
        public IPEndPoint RemoteEndpoint
        {
            get { return (IPEndPoint) Client.Client.RemoteEndPoint; }
        }

        /// <summary>
        /// Gets the underlying TcpClient
        /// </summary>
        public TcpClient Client { get; private set; }

        /// <summary>
        /// Writes the message to the remote host
        /// </summary>
        /// <param name="value">The data to write</param>
        public void Write(T value)
        {
            if (value == null)
            {
                throw new ArgumentNullException("value");
            }
            if (_disposed)
            {
                Exceptions.Throw(new IllegalStateException("Link has been closed."), Logger);
            }

            _writer.WriteString(value.GetType().AssemblyQualifiedName);
            value.Write(_writer);
        }

        /// <summary>
        /// Writes the value to this link asynchronously
        /// </summary>
        /// <param name="value">The data to write</param>
        /// <param name="token">The cancellation token</param>
        public async Task WriteAsync(T value, CancellationToken token)
        {
            if (_disposed)
            {
                Exceptions.Throw(new IllegalStateException("Link has been closed."), Logger);
            }

            await _writer.WriteStringAsync(value.GetType().AssemblyQualifiedName, token);
            await value.WriteAsync(_writer, token);
        }

        /// <summary>
        /// Reads the value from the link synchronously
        /// </summary>
        public T Read()
        {
            if (_disposed)
            {
                Exceptions.Throw(new IllegalStateException("Link has been disposed."), Logger);
            }

            string dataType = _reader.ReadString();

            if (dataType == null)
            {
                return default(T);
            }

            T value = _cache.GetInstance(dataType);

            if (value == null)
            {
                return default(T);
            }
            
            value.Read(_reader);
            return value;
        }

        /// <summary>
        /// Reads the value from the link asynchronously
        /// </summary>
        /// <param name="token">The cancellation token</param>
        public async Task<T> ReadAsync(CancellationToken token)
        {
            if (_disposed)
            {
                Exceptions.Throw(new IllegalStateException("Link has been disposed."), Logger);
            }

            string dataType = "";

            dataType = await _reader.ReadStringAsync(token);

            if (dataType == null)
            {
                return default(T);
            }

            T value = _cache.GetInstance(dataType);

            if(value==null)
            {
                return default(T);
            }

            await value.ReadAsync(_reader, token);
            return value;
        }

        /// <summary>
        /// Close the client connection
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Subclasses of Links should overwrite this to handle disposing
        /// of the link
        /// </summary>
        /// <param name="disposing">To dispose or not</param>
        public virtual void Dispose(bool disposing)
        {
            if (_disposed)
            {
                return;
            }

            if (disposing)
            {
                try
                {
                    Client.GetStream().Close();
                }
                catch (InvalidOperationException)
                {
                    Logger.Log(Level.Warning, "failed to close stream on a non-connected socket.");
                }

                Client.Close();
            }
            _disposed = true;
        }

        /// <summary>
        /// Overrides Equals. Two Link objects are equal if they are connected
        /// to the same remote endpoint.
        /// </summary>
        /// <param name="obj">The object to compare</param>
        /// <returns>True if the object is equal to this Link, otherwise false</returns>
        public override bool Equals(object obj)
        {
            Link<T> other = obj as Link<T>;
            if (other == null)
            {
                return false;
            }

            return other.RemoteEndpoint.Equals(RemoteEndpoint);
        }

        /// <summary>
        /// Gets the hash code for the Link object.
        /// </summary>
        /// <returns>The object's hash code</returns>
        public override int GetHashCode()
        {
            return RemoteEndpoint.GetHashCode();
        }

        /// <summary>
        /// Discovers the IPEndpoint for the current machine.
        /// </summary>
        /// <returns>The local IPEndpoint</returns>
        private IPEndPoint GetLocalEndpoint()
        {
            IPAddress address = NetworkUtils.LocalIPAddress;
            int port = ((IPEndPoint) Client.Client.LocalEndPoint).Port;
            return new IPEndPoint(address, port);
        }
    }
}