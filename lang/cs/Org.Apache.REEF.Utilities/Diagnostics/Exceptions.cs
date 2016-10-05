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
using System.Runtime.ExceptionServices;
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Utilities.Diagnostics
{
    [Private]
    [Obsolete("TODO[JIRA REEF-1467] This class will be removed")]
    public static class Exceptions
    {
        #region methods
        /// <summary>
        /// Call this method to throw an exception.
        /// </summary>
        /// <remarks>
        /// Calling this method will trace the exception and do other common processing, 
        /// and then it will throw the exception. This method traces the exception type 
        /// and message at error level and the full stack trace at all other levels.
        /// </remarks>
        /// <example>
        ///     Exceptions.Throw(new Exception("Some exception"));
        /// </example>
        /// <param name="exception">The exception to be thrown.</param>
        /// <param name="message">The message from the caller class.</param>
        /// <param name="logger">The logger from the caller class.</param>
        public static void Throw(Exception exception, string message, Logger logger)
        {
            string logMessage = string.Concat(DiagnosticsMessages.ExceptionThrowing, " ", exception.GetType().Name, " ", message);
            if (logger == null)
            {
                Console.WriteLine("Exception caught before logger is initiated, error message: " + logMessage + exception.Message);
            }
            else
            {
                logger.Log(Level.Error, logMessage, exception);
            }
            ExceptionDispatchInfo.Capture(exception).Throw();
        }

        /// <summary>
        /// Call this method to throw an exception.
        /// </summary>
        /// <remarks>
        /// Calling this method will trace the exception and do other common processing, 
        /// and then it will throw the exception. This method traces the exception type 
        /// and message at error level and the full stack trace at all other levels.
        /// </remarks>
        /// <example>
        ///     Exceptions.Throw(new Exception("Some exception"));
        /// </example>
        /// <param name="exception">The exception to be thrown.</param>
        /// <param name="logger">The logger of the caller class.</param>
        public static void Throw(Exception exception, Logger logger)
        {
            Throw(exception, string.Empty, logger);
        }

        /// <summary>
        /// Call this method every time when an exception is caught.
        /// </summary>
        /// <remarks>
        /// Calling this method will trace the exception and do other common processing.
        /// This method traces the exception type and message at error level and the full
        /// stack trace at all other levels.
        /// </remarks>
        /// <example>
        ///     try
        ///     {
        ///         // Some code that can throw
        ///     }
        ///     catch (Exception e)
        ///     {
        ///         Exceptions.Caught(e);
        ///         // Exception handling code
        ///     }
        /// </example>
        /// <param name="exception">The exception being caught.</param>
        /// <param name="level">The log level.</param>
        /// <param name="logger">The logger from the caller class.</param>
        public static void Caught(Exception exception, Level level, Logger logger)
        {
            Caught(exception, level, string.Empty, logger);
        }

        /// <summary>
        /// Call this method every time when an exception is caught.
        /// </summary>
        /// <remarks>
        /// Calling this method will trace the exception and do other common processing.
        /// This method traces the exception type and message at error level and the full
        /// stack trace at all other levels.
        /// </remarks>
        /// <example>
        ///     try
        ///     {
        ///         // Some code that can throw
        ///     }
        ///     catch (Exception e)
        ///     {
        ///         Exceptions.Caught(e);
        ///         // Exception handling code
        ///     }
        /// </example>
        /// <param name="exception">The exception being caught.</param>
        /// <param name="level">The log level.</param>
        /// <param name="message">The additional message to log.</param>
        /// <param name="logger">The Logger from the caller class.</param>
        public static void Caught(Exception exception, Level level, string message, Logger logger)
        {
            string logMessage = string.Concat(DiagnosticsMessages.ExceptionCaught, " ", exception.GetType().Name, " ", message);
            if (logger == null)
            {
                Console.WriteLine("Exception caught before logger is initiated, error message: " + logMessage + exception.Message);
            }
            else
            {
                logger.Log(level, logMessage, exception);
            }
        }

        public static void CaughtAndThrow(Exception exception, Level level, Logger logger)
        {
            CaughtAndThrow(exception, level, string.Empty, logger);
        }

        public static void CaughtAndThrow(Exception exception, Level level, string message, Logger logger)
        {
            string logMessage = string.Concat(DiagnosticsMessages.ExceptionCaught, " ", exception.GetType().Name, " ", message);
            if (logger == null)
            {
                Console.WriteLine("Exception caught before logger is initiated, error message: " + logMessage + exception.Message);
            }
            else
            {
                logger.Log(level, logMessage, exception);
            }
            ExceptionDispatchInfo.Capture(exception).Throw();
        }
        #endregion
    }
}
