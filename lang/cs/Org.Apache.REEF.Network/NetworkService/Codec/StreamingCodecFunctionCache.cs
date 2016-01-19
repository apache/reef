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
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Wake.StreamingCodec;

namespace Org.Apache.REEF.Network.NetworkService.Codec
{
    /// <summary>
    /// Cache of StreamingCodec functions used to store codec functions for messages
    /// to avoid reflection cost. Each message type is assumed to have a unique 
    /// associated codec
    /// </summary>
    /// <typeparam name="T">The message type</typeparam>
    internal class StreamingCodecFunctionCache<T>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(StreamingCodecFunctionCache<T>));
        private readonly ConcurrentDictionary<Type, Func<IDataReader, T>> _readFuncCache;
        private readonly ConcurrentDictionary<Type, Func<IDataReader, CancellationToken, T>> _readAsyncFuncCache;
        private readonly ConcurrentDictionary<Type, Action<T, IDataWriter>> _writeFuncCache;
        private readonly ConcurrentDictionary<Type, Func<T, IDataWriter, CancellationToken, Task>> _writeAsyncFuncCache;
        private readonly IInjector _injector;
        private readonly Type _streamingCodecType;
        private readonly object _lock;

        /// <summary>
        /// Create new StreamingCodecFunctionCache.
        /// </summary>
        /// <param name="injector"> Injector</param>
        internal StreamingCodecFunctionCache(IInjector injector)
        {
            _injector = injector;
            _readFuncCache = new ConcurrentDictionary<Type, Func<IDataReader, T>>();
            _readAsyncFuncCache = new ConcurrentDictionary<Type, Func<IDataReader, CancellationToken, T>>();
            _writeFuncCache = new ConcurrentDictionary<Type, Action<T, IDataWriter>>();
            _writeAsyncFuncCache = new ConcurrentDictionary<Type, Func<T, IDataWriter, CancellationToken, Task>>();
            _streamingCodecType = typeof(IStreamingCodec<>);
            _lock = new object();
        }

        /// <summary>
        /// Creates the read delegate function of StreamingCodec from the message type
        /// </summary>
        /// <param name="messageType">Type of message</param>
        /// <returns>The read delegate function</returns>
        internal Func<IDataReader, T> ReadFunction(Type messageType)
        {
            Func<IDataReader, T> readFunc;

            if (!_readFuncCache.TryGetValue(messageType, out readFunc))
            {
                AddCodecFunctions(messageType);
                readFunc = _readFuncCache[messageType];
            }

            return readFunc;
        }

        /// <summary>
        /// Creates the read async delegate function of StreamingCodec from the message type
        /// </summary>
        /// <param name="messageType">Type of message</param>
        /// <returns>The read async delegate function</returns>
        internal Func<IDataReader, CancellationToken, T> ReadAsyncFunction(Type messageType)
        {
            Func<IDataReader, CancellationToken, T> readFunc;

            if (!_readAsyncFuncCache.TryGetValue(messageType, out readFunc))
            {
                AddCodecFunctions(messageType);
                readFunc = _readAsyncFuncCache[messageType];
            }

            return readFunc;
        }

        /// <summary>
        /// Creates the write delegate function of StreamingCodec from the message type
        /// </summary>
        /// <param name="messageType">Type of message</param>
        /// <returns>The write delegate function</returns>
        internal Action<T, IDataWriter> WriteFunction(Type messageType)
        {
            Action<T, IDataWriter> writeFunc;

            if (!_writeFuncCache.TryGetValue(messageType, out writeFunc))
            {
                AddCodecFunctions(messageType);
                writeFunc = _writeFuncCache[messageType];
            }

            return writeFunc;
        }

        /// <summary>
        /// Creates the write async delegate function of StreamingCodec from the message type
        /// </summary>
        /// <param name="messageType">Type of message</param>
        /// <returns>The write async delegate function</returns>
        internal Func<T, IDataWriter, CancellationToken, Task> WriteAsyncFunction(Type messageType)
        {
            Func<T, IDataWriter, CancellationToken, Task> writeFunc;

            if (!_writeAsyncFuncCache.TryGetValue(messageType, out writeFunc))
            {
                AddCodecFunctions(messageType);
                writeFunc = _writeAsyncFuncCache[messageType];
            }

            return writeFunc;
        }

        private void AddCodecFunctions(Type messageType)
        {
            if (!typeof(T).IsAssignableFrom(messageType))
            {
                Exceptions.CaughtAndThrow(new Exception("Message type not assignable to base type"), Level.Error,
                    Logger);
            }

            lock (_lock)
            {
                Type codecType = _streamingCodecType.MakeGenericType(messageType);
                var codec = _injector.GetInstance(codecType);

                MethodInfo readMethod = codec.GetType().GetMethod("Read");
                _readFuncCache[messageType] = 
                    (Func<IDataReader, T>)Delegate.CreateDelegate(typeof(Func<IDataReader, T>), codec, readMethod);

                MethodInfo readAsyncMethod = codec.GetType().GetMethod("ReadAsync");
                MethodInfo genericHelper = GetType()
                    .GetMethod("ReadAsyncHelperFunc", BindingFlags.NonPublic | BindingFlags.Instance);
                MethodInfo constructedHelper = genericHelper.MakeGenericMethod(messageType);
                _readAsyncFuncCache[messageType] =
                    (Func<IDataReader, CancellationToken, T>)
                        constructedHelper.Invoke(this, new[] { readAsyncMethod, codec });

                MethodInfo writeMethod = codec.GetType().GetMethod("Write");
                genericHelper = GetType().GetMethod("WriteHelperFunc", BindingFlags.NonPublic | BindingFlags.Instance);
                constructedHelper = genericHelper.MakeGenericMethod(messageType);
                _writeFuncCache[messageType] =
                    (Action<T, IDataWriter>)constructedHelper.Invoke(this, new[] { writeMethod, codec });

                MethodInfo writeAsyncMethod = codec.GetType().GetMethod("WriteAsync");
                genericHelper = GetType()
                    .GetMethod("WriteAsyncHelperFunc", BindingFlags.NonPublic | BindingFlags.Instance);
                constructedHelper = genericHelper.MakeGenericMethod(messageType);
                _writeAsyncFuncCache[messageType] =
                    (Func<T, IDataWriter, CancellationToken, Task>)
                        constructedHelper.Invoke(this, new[] { writeAsyncMethod, codec });
            }
        }

        private Action<T, IDataWriter> WriteHelperFunc<T1>(MethodInfo method, object codec) where T1 : class
        {
            Action<T1, IDataWriter> func = 
                (Action<T1, IDataWriter>)Delegate.CreateDelegate(typeof(Action<T1, IDataWriter>), codec, method);

            Action<T, IDataWriter> ret = (obj, writer) => func(obj as T1, writer);
            return ret;
        }

        private Func<T, IDataWriter, CancellationToken, Task> WriteAsyncHelperFunc<T1>(MethodInfo method, object codec)
            where T1 : class
        {
            Func<T1, IDataWriter, CancellationToken, Task> func =
                (Func<T1, IDataWriter, CancellationToken, Task>)
                Delegate.CreateDelegate(typeof(Func<T1, IDataWriter, CancellationToken, Task>), codec, method);

            Func<T, IDataWriter, CancellationToken, Task> ret = (obj, writer, token) => func(obj as T1, writer, token);
            return ret;
        }

        private Func<IDataReader, CancellationToken, T> ReadAsyncHelperFunc<T1>(MethodInfo method, object codec)
            where T1 : class
        {
            Func<IDataReader, CancellationToken, Task<T1>> func =
                (Func<IDataReader, CancellationToken, Task<T1>>)
                Delegate.CreateDelegate(typeof(Func<IDataReader, CancellationToken, Task<T1>>), codec, method);

            Func<IDataReader, CancellationToken, T1> func1 = (writer, token) => func(writer, token).Result;
            Func<IDataReader, CancellationToken, T> func2 = (writer, token) => ((T)(object)func1(writer, token));
            return func2;
        }
    }
}
