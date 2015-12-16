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
using System.Linq;
using Org.Apache.REEF.Common.Avro;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Driver.Bridge
{
    /// <summary>
    ///  HttpServerHandler, the handler for all CLR http events
    /// </summary>
    internal sealed class HttpServerHandler : IObserver<IHttpMessage>
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(HttpServerHandler));

        private static readonly string SPEC = "SPEC";

        private readonly IDictionary<string, IHttpHandler> eventHandlers = new Dictionary<string, IHttpHandler>();

        private readonly HttpServerPort httpServerPort;

        /// <summary>
        /// Initializes a new instance of the <see cref="HttpServerHandler" /> class.
        /// </summary>
        /// <param name="httpEventHandlers">The HTTP event handlers.</param>
        /// <param name="httpServerPort">The HTTP server port.</param>
        [Inject]
        private HttpServerHandler([Parameter(Value = typeof(DriverBridgeConfigurationOptions.HttpEventHandlers))] ISet<IHttpHandler> httpEventHandlers,
                                 HttpServerPort httpServerPort)
        {
            LOGGER.Log(Level.Info, "Constructing HttpServerHandler");       
            foreach (var h in httpEventHandlers)
            {
                string spec = h.GetSpecification();
                if (spec.Contains(":"))
                {
                    Exceptions.Throw(new ArgumentException("spec cannot contain :"), "The http spec given is " + spec, LOGGER);
                }
                LOGGER.Log(Level.Info, "HttpHandler spec:" + spec);   
                eventHandlers.Add(spec.ToLower(CultureInfo.CurrentCulture), h);
            }
            this.httpServerPort = httpServerPort;
        }

        /// <summary>
        /// Called when receiving an http request from Java side
        /// </summary>
        /// <param name="httpMessage">The HTTP message.</param>
        public void OnNext(IHttpMessage httpMessage)
        {
            LOGGER.Log(Level.Info, "HttpHandler OnNext is called");
            string requestString = httpMessage.GetRequestString();

            if (requestString != null && requestString.Equals(SPEC))
            {
                LOGGER.Log(Level.Info, "HttpHandler OnNext, requestString:" + requestString);
                LOGGER.Log(Level.Info, "HttpHandler OnNext, port number:" + httpServerPort.PortNumber);

                httpMessage.SetUriSpecification(GetAllSpecifications());
            }
            else
            {
                LOGGER.Log(Level.Info, "HttpHandler OnNext, handling http request.");
                byte[] byteData = httpMessage.GetQueryReuestData();
                AvroHttpRequest avroHttpRequest = AvroHttpSerializer.FromBytesWithJson(byteData);
                LOGGER.Log(Level.Info, string.Format(CultureInfo.CurrentCulture, "HttpHandler OnNext, requestData:", avroHttpRequest));

                string spec = GetSpecification(avroHttpRequest.PathInfo);
                if (spec != null)
                {
                    LOGGER.Log(Level.Info, "HttpHandler OnNext, target:" + spec);
                    ReefHttpRequest request = ToHttpRequest(avroHttpRequest);

                    foreach (var h in request.Header)
                    {
                        LOGGER.Log(Level.Info, string.Format(CultureInfo.CurrentCulture, "HttpRequest Header-key: {0}, value: {1}.", h.Key, h.Value));
                    }
                    ReefHttpResponse response = new ReefHttpResponse();

                    IHttpHandler handler;
                    eventHandlers.TryGetValue(spec.ToLower(CultureInfo.CurrentCulture), out handler);

                    byte[] responseData;
                    if (handler != null)
                    {
                        LOGGER.Log(Level.Info, "HttpHandler OnNext, get eventHandler:" + handler.GetSpecification());
                        handler.OnHttpRequest(request, response);
                        responseData = response.OutputStream;
                    }
                    else
                    {
                        responseData =
                            ByteUtilities.StringToByteArrays(string.Format(CultureInfo.CurrentCulture, "No event handler found at CLR side for {0}.", spec));
                    }
                    httpMessage.SetQueryResponseData(responseData);
                }
            }
        }

        public void OnCompleted()
        {
            throw new NotImplementedException();
        }

        public void OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        private string GetAllSpecifications()
        {
            return string.Join(":", eventHandlers.Keys.ToArray());
        }

        private string GetSpecification(string requestUri)
        {
            if (requestUri != null)
            {
                string[] parts = requestUri.Split('/');

                if (parts.Length > 1)
                {
                    return parts[1];
                }
            }
            return null;            
        }

        private ReefHttpRequest ToHttpRequest(AvroHttpRequest avroRequest)
        {
            ReefHttpRequest httpRequest = new ReefHttpRequest();
            httpRequest.PathInfo = avroRequest.PathInfo;
            httpRequest.InputStream = avroRequest.InputStream;
            httpRequest.Url = avroRequest.RequestUrl;
            httpRequest.Querystring = avroRequest.QueryString;

            IDictionary<string, string> header = new Dictionary<string, string>();
            foreach (var h in avroRequest.Header)
            {
                header.Add(h.key, h.value);
            }
            httpRequest.Header = header;

            HttpMethod m;
            HttpMethod.TryParse(avroRequest.HttpMethod, true, out m);
            httpRequest.Method = m;
            return httpRequest;
        }    
    }
}