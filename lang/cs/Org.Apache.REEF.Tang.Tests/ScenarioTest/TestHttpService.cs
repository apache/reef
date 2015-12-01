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
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Xunit;

namespace Org.Apache.REEF.Tang.Tests.ScenarioTest
{
    public class TestHttpService
    {
        [Fact]
        public void HttpEventHandlersTest()
        {
            ConfigurationModule module =
                new ConfigurationModuleBuilder()
                .BindSetEntry<HttpEventHandlers, HttpServerReefEventHandler, IHttpHandler>(GenericType<HttpEventHandlers>.Class, GenericType<HttpServerReefEventHandler>.Class)
                .BindSetEntry<HttpEventHandlers, HttpServerNrtEventHandler, IHttpHandler>(GenericType<HttpEventHandlers>.Class, GenericType<HttpServerNrtEventHandler>.Class)
                .Build();

           IConfiguration c = module.Build();
           var service = TangFactory.GetTang().NewInjector(c).GetInstance<HttpServer>();
           Assert.NotNull(service);

           var j = TangFactory.GetTang().NewInjector(c).GetInstance<HttpRunTimeStartHandler>();
           Assert.NotNull(j);
        }

        [Fact]
        public void RuntimeStartHandlerTest()
        {
            ConfigurationModule module =
                new ConfigurationModuleBuilder()
                    .BindSetEntry<RuntimeStartHandler, HttpRunTimeStartHandler, IObserver<RuntimeStart>>(
                        GenericType<RuntimeStartHandler>.Class, GenericType<HttpRunTimeStartHandler>.Class)
                    .Build();
            IConfiguration clockConfiguraiton = module.Build();

            RuntimeClock clock = TangFactory.GetTang().NewInjector(clockConfiguraiton).GetInstance<RuntimeClock>();
            var rh = clock.ClockRuntimeStartHandler.Get();
            Assert.Equal(rh.Count, 1);
            foreach (var e in rh)
            {
                Assert.True(e is HttpRunTimeStartHandler);
                HttpRunTimeStartHandler r = (HttpRunTimeStartHandler)e;
                var s = r.Server;
                Assert.Equal(s.JettyHandler.HttpeventHanlders.Count, 0); // no handlers are bound
            }
        }

        [Fact]
        public void RuntimeStartStopHandlerTest()
        {
            IConfiguration clockConfiguraiton = HttpRuntimeConfiguration.CONF.Build();
            RuntimeClock clock = TangFactory.GetTang().NewInjector(clockConfiguraiton).GetInstance<RuntimeClock>();
            var starts = clock.ClockRuntimeStartHandler.Get();
            var stops = clock.ClockRuntimeStopHandler.Get();

            HttpRunTimeStartHandler start = null;
            HttpRunTimeStopHandler stop = null;

            Assert.Equal(starts.Count, 1);
            foreach (var e in starts)
            {
                Assert.True(e is HttpRunTimeStartHandler);
                start = (HttpRunTimeStartHandler)e;
            }

            Assert.Equal(stops.Count, 1);
            foreach (var e in stops)
            {
                Assert.True(e is HttpRunTimeStopHandler);
                stop = (HttpRunTimeStopHandler)e;
            }

            Assert.Equal(start.Server, stop.Server);
            Assert.Equal(start.Server.JettyHandler.HttpeventHanlders, stop.Server.JettyHandler.HttpeventHanlders);
            Assert.Same(start.Server, stop.Server); 
        }

        [Fact]
        public void RuntimeStartHandlerMergeTest()
        {
            IConfiguration clockConfiguraiton = HttpHandlerConfiguration.CONF
                .Set(HttpHandlerConfiguration.P,
                     GenericType<HttpServerReefEventHandler>.Class)
                .Set(HttpHandlerConfiguration.P,
                     GenericType<HttpServerNrtEventHandler>.Class)
                .Build();
                                       
            RuntimeClock clock = TangFactory.GetTang().NewInjector(clockConfiguraiton).GetInstance<RuntimeClock>();

            var rh = clock.ClockRuntimeStartHandler.Get();
            Assert.Equal(rh.Count, 1);
            foreach (var e in rh)
            {
                Assert.True(e is HttpRunTimeStartHandler);
                HttpRunTimeStartHandler r = (HttpRunTimeStartHandler)e;
                var s = r.Server;
                foreach (IHttpHandler h in s.JettyHandler.HttpeventHanlders)
                {
                    System.Diagnostics.Debug.WriteLine(h.GetUriSpecification());
                }
            }
        }
    }

    public class HttpRequest
    {        
    }

    public class Httpresponse
    {        
    }

    public class HttpServerReefEventHandler : IHttpHandler
    {
        [Inject]
        public HttpServerReefEventHandler()
        {
        }

        public string GetUriSpecification()
        {
            return "/Reef";
        }

        public void OnHttpRequest(HttpRequest request, Httpresponse response)
        {
            // handle the event
        }
    }

    public class HttpServerNrtEventHandler : IHttpHandler
    {
        [Inject]
        public HttpServerNrtEventHandler()
        {            
        }

        public string GetUriSpecification()
        {
            return "/NRT";
        }

        public void OnHttpRequest(HttpRequest request, Httpresponse response)
        {
        }
    }

    public class Server
    {
        public Server(int port)
        {          
        }

        public void Start()
        {           
        }

        public void Stop()
        {
        }

        public void Join()
        {
        }

        public void SetHandler(JettyHandler handler)
        {           
        }
    }
}