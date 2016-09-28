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
using System.Diagnostics;
using System.Globalization;

namespace Org.Apache.REEF.Utilities.Logging
{
    public sealed class Logger
    {
        private static readonly string[] LogLevel = new string[]
            {
                "OFF",
                "ERROR",
                "WARNING",
                "START",
                "EXIT",
                "INFO",
                "VERBOSE"
            };

        private static readonly Dictionary<Level, TraceEventType> EventTypes
            = new Dictionary<Level, TraceEventType>()
                    {
                        { Level.Off, TraceEventType.Stop },
                        { Level.Error, TraceEventType.Error },
                        { Level.Warning, TraceEventType.Warning },
                        { Level.Start, TraceEventType.Start },
                        { Level.Stop, TraceEventType.Stop },
                        { Level.Info, TraceEventType.Information },
                        { Level.Verbose, TraceEventType.Verbose },
                    };

        private static Level _customLevel = Level.Info;

        private Level _instanceLevel = Level.Unset;

        private static List<TraceListener> _traceListeners;

        private readonly string _name;       

        private readonly TraceSource _traceSource;

        private Logger(string name)
        {
            _name = name;
            _traceSource = new TraceSource(_name, SourceLevels.All);
            if (TraceListeners.Count == 0)
            {
                // before customized listener is added, we would need to log to console
                _traceSource.Listeners.Add(new TextWriterTraceListener(Console.Out));
            }
            else
            {
                _traceSource.Listeners.Clear();
                foreach (TraceListener listener in TraceListeners)
                {
                    _traceSource.Listeners.Add(listener);
                }  
            }
        }

        public static Level CustomLevel
        {
            get
            {
                return _customLevel;
            }

            set
            {
                _customLevel = value;
            }
        }

        public Level InstanceLevel
        {
            get
            {
                return _instanceLevel;
            }

            set
            {
                _instanceLevel = value;
            }
        }

        public static List<TraceListener> TraceListeners
        {
            get
            {
                if (_traceListeners == null)
                {
                    _traceListeners = new List<TraceListener>();
                }
                return _traceListeners;
            }
        }

        public static void SetCustomLevel(Level customLevel)
        {
            _customLevel = customLevel;
        }

        public static void AddTraceListener(TraceListener listener)
        {
            TraceListeners.Add(listener);
        }

        public static Logger GetLogger(Type type)
        {
            return GetLogger(type.FullName);
        }

        public static Logger GetLogger(string name)
        {
            return new Logger(name);
        }

        /// <summary>
        /// Determines whether or not the current log level will be logged by the logger.
        /// </summary>
        public bool IsLoggable(Level level)
        {
            return (InstanceLevel != Level.Unset ? InstanceLevel : CustomLevel) >= level;
        }

        /// <summary>
        /// Log the message with the specified Log Level.
        ///
        /// If addtional arguments are passed, the message will be treated as
        /// a format string.  The format string and the additional arguments 
        /// will be formatted according to string.Format()
        /// </summary>
        /// <param name="level"></param>
        /// <param name="formatStr"></param>
        /// <param name="args"></param>
        public void Log(Level level, string formatStr, params object[] args)
        {
            if (IsLoggable(level))
            {
                string msg = FormatMessage(formatStr, args);
                string logMessage = 
                    DateTime.Now.ToString("o", CultureInfo.InvariantCulture) 
                    + " " 
                    + System.Threading.Thread.CurrentThread.ManagedThreadId.ToString("D4", CultureInfo.InvariantCulture) 
                    + Environment.NewLine + LogLevel[(int)level] + ": " 
                    + msg;

                _traceSource.TraceEvent(
                    EventTypes[level],
                    0, // we don't use event id for now, but this can be useful for e2e logging later  
                    logMessage);
            }
        }

        public void Log(Level level, string msg, Exception exception)
        {
            string exceptionLog;

            if (exception != null)
            {
                exceptionLog = string.Format(
                    CultureInfo.InvariantCulture,
                    "\r\nEncountered error [{0}]",
                    exception);
            }
            else
            {
                exceptionLog = string.Empty;
            }

            if (IsLoggable(level))
            {
                Log(level, msg + exceptionLog);
            }
        }

        public IDisposable LogFunction(string function, params object[] args)
        {
            return LogScope(function, args);
        }

        public IDisposable LogScope(string format, params object[] args)
        {
            return new LoggingScope(this, DateTime.Now + " " + format, args);
        }

        private string FormatMessage(string formatStr, params object[] args)
        {
            return args.Length > 0 ? string.Format(CultureInfo.CurrentCulture, formatStr, args) : formatStr;
        }

        /// <summary>
        /// Represents a logging scope.
        /// </summary>
        /// <remarks>
        /// A start log is written when an instance is created 
        /// and a stop trace is written when the instance is disposed.
        /// </remarks>
        private sealed class LoggingScope : IDisposable
        {
            private readonly Stopwatch _stopWatch;

            private readonly Logger _logger;

            private readonly string _content;

            /// <summary>
            /// Initializes a new instance of the LoggingScope class. 
            /// </summary>
            /// <param name="logger"></param>
            /// <param name="format"></param>
            /// <param name="args"></param>
            public LoggingScope(Logger logger, string format, params object[] args)
            {
                _logger = logger;

                _stopWatch = Stopwatch.StartNew();

                string content  = args.Length > 0 ? string.Format(CultureInfo.InvariantCulture, format, args) : format;
                _content = content;

                _logger.Log(Level.Start, content);
            }

            /// <summary>
            /// Logs the end of a scope.
            /// </summary>
            public void Dispose()
            {
                _logger.Log(Level.Stop, string.Format(CultureInfo.InvariantCulture, "{0}. Duration: [{1}].", _content, _stopWatch.Elapsed));
                _stopWatch.Stop();
            }
        }
    }
}
