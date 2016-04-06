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
using System.Threading;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Network.Group.Driver.Impl
{
    /// <summary>
    /// This class is to manage the active contexts in group communications
    /// </summary>
    public class ContextManager
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(ContextManager));
        private readonly ReaderWriterLockSlim _contextLock = new ReaderWriterLockSlim();
        private readonly int _totalNumberOfContexts;
        private int _contextsAdded;
        private readonly IDictionary<string, IActiveContext> _activeContexts;

        public ContextManager(int numberOfContexts)
        {
            _totalNumberOfContexts = numberOfContexts;
            _contextsAdded = 0;
            _activeContexts = new Dictionary<string, IActiveContext>();
        }

        /// <summary>
        /// Get number of active context in the collection
        /// </summary>
        public int Count
        {
            get { return _contextsAdded; }
        }

        /// <summary>
        /// Get all current active context in the collection
        /// </summary>
        public ICollection<IActiveContext> ActiveContexts
        {
            get
            {
                return _activeContexts.Values;
            }
        }

        /// <summary>
        /// Add an active context to the collection
        /// </summary>
        /// <param name="context"></param>
        public bool AddContext(IActiveContext context)
        {
            _contextLock.EnterWriteLock();
            try
            {
                if (_activeContexts.ContainsKey(context.Id))
                {
                    Exceptions.Throw(new Exception("The context is already in the collection" + context.Id), Logger);
                }

                _activeContexts.Add(context.Id, context);

                if (Interlocked.Increment(ref _contextsAdded) == _totalNumberOfContexts)
                {
                    return true;
                }
                return false;
            }
            finally
            {
                _contextLock.ExitWriteLock();
            }
        }

        /// <summary>
        /// Remove an active context from the collection 
        /// </summary>
        /// <param name="contextId"></param>
        public void RemoveContext(string contextId)
        {
            _contextLock.EnterWriteLock();
            try
            {
                if (_activeContexts.ContainsKey(contextId))
                {
                    _activeContexts.Remove(contextId);
                    _contextsAdded = _contextsAdded - 1;
                }
                else
                {
                    Exceptions.Throw(new ArgumentException("The context to remove is not in collection" + contextId), Logger);
                }
            }
            finally
            {
               _contextLock.ExitWriteLock(); 
            }
        }

        /// <summary>
        /// Get an active context by id
        /// </summary>
        /// <param name="contextId"></param>
        /// <returns></returns>
        public IActiveContext GetContext(string contextId)
        {
            IActiveContext context = null;
            _contextLock.EnterReadLock();
            try
            {
                _activeContexts.TryGetValue(contextId, out context);
                if (context == null)
                {
                    Exceptions.Throw(new ArgumentException("The context doesn't exist in the collection:" + contextId), Logger);
                }
            }
            finally
            {
                _contextLock.ExitReadLock();
            }
            return context;
        }

        ~ContextManager()
        {
            if (_contextLock != null)
            {
                _contextLock.Dispose();
            }
        }
    }
}
