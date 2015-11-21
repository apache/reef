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
using System.Runtime.ExceptionServices;
using System.Text;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Utilities.Diagnostics
{
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

        /// <summary>
        /// This method returns true if the exception passed as parameter is a critical exception
        /// that should have not been caught. Examples for such exceptions are StackOverflowException
        /// and OutOfMemoryException.
        /// </summary>
        /// <remarks>
        /// Catch statements which catch all exceptions must call this method immediately and rethrow
        /// without further processing if the method returns true.
        /// </remarks>
        /// <example>
        /// try
        /// {
        ///     // Some code that can throw
        /// }
        /// catch (Exception e)
        /// {
        ///     if (Exceptions.MustRethrow(e))
        ///     {
        ///         throw;
        ///     }
        ///     // Exception handling code
        /// }
        /// </example>
        /// <param name="exception">The exception to be checked.</param>
        /// <returns>True if the exceptions is critical one and should not be caught and false otherwise.</returns>
        public static bool MustRethrow(Exception exception)
        {
            return exception is OutOfMemoryException ||
                   exception is StackOverflowException;
        }

        /// <summary>
        /// Gets an exception message that includes the messages of the inner exceptions..
        /// </summary>
        /// <param name="e">The exception.</param>
        /// <returns>The message</returns>
        public static string GetFullMessage(Exception e)
        {
            var fullMessage = new StringBuilder();
            bool firstLevel = true;
            while (e != null)
            {
                if (firstLevel)
                {
                    firstLevel = false;
                }
                else
                {
                    fullMessage.Append("-->");
                }
                fullMessage.Append(e.Message);
                e = e.InnerException;
            }

            return fullMessage.ToString();
        }

        /// <summary>
        /// Call this method to throw ArgumentException for an invalid argument.
        /// </summary>
        /// <param name="argumentName">The invalid argument name.</param>
        /// <param name="message">A message explaining the reason for th exception.</param>
        /// <param name="logger">The logger of the caller class.</param>
        public static void ThrowInvalidArgument(string argumentName, string message, Logger logger)
        {
            Throw(new ArgumentException(message, argumentName), logger);
        }

        /// <summary>
        /// Call this method to throw ArgumentOutOfRangeException exception.
        /// </summary>
        /// <param name="argumentName">The invalid argument name.</param>
        /// <param name="message">A message explaining the reason for th exception.</param>
        /// <param name="logger">The logger of the caller class.</param>
        public static void ThrowArgumentOutOfRange(string argumentName, string message, Logger logger)
        {
            Throw(new ArgumentOutOfRangeException(argumentName, message), logger);
        }

        /// <summary>
        /// Call this method to check if an argument is null and throw ArgumentNullException exception.
        /// </summary>
        /// <param name="argument">The argument to be checked.</param>
        /// <param name="name">The name of the argument.</param>
        /// <param name="logger">The logger of the caller class.</param>
        public static void ThrowIfArgumentNull(object argument, string name, Logger logger)
        {
            if (argument == null)
            {
                Exceptions.Throw(new ArgumentNullException(name), logger);
            }
        }

        /// <summary>
        /// Call this method to throw ObjectDisposedException if an object is disposed.
        /// </summary>
        /// <remarks>
        /// All disposable objects should check their state and throw in the beginning of each public method.
        /// This helper method provides a shorter way to do this.
        /// </remarks>
        /// <example>
        /// class SomeClass : IDisposable
        /// {
        ///     bool _disposed;
        ///     // ...
        ///     public void SomePublicMethod()
        ///     {
        ///         Exceptions.ThrowIfObjectDisposed(_disposed, this);
        ///         // Method's code
        ///     }
        /// }
        /// </example>
        /// <param name="disposed">True if the object is disposed.</param>
        /// <param name="o">The object.</param>
        /// <param name="logger">The logger of the caller class.</param>
        public static void ThrowIfObjectDisposed(bool disposed, object o, Logger logger)
        {
            if (disposed)
            {
                Throw(new ObjectDisposedException(o.GetType().Name), logger);
            }
        }
        #endregion
    }
}
