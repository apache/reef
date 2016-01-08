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
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using NSubstitute;
using Org.Apache.REEF.Client.YARN.RestClient;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.Client.Tests
{
    [TestClass]
    public class RestClientTests
    {
        private const string AnyResource = "anyResource";
        private const string AnyRootElement = "anyRootElement";
        private const int AnyIntField = 42;
        private const string AnyStringField = "AnyStringFieldValue";
        private const string MediaType = @"application/json";

        private static readonly string ExpectedReturnJson =
            "{anyRootElement:{\"AnyStringField\":\"" + AnyStringField + "\",\"AnyIntField\":" + AnyIntField + "}}";

        private static readonly string AnyPostContent =
            "{\"AnyStringField\":\"" + AnyStringField + "\"}";

        private static readonly Uri AnyRequestUri = new Uri("http://any/request/uri");
        private static readonly Encoding Encoding = Encoding.UTF8;

        [TestMethod]
        public async Task RestClientGetRequestReturnsResponse()
        {
            var tc = new TestContext();

            var client = tc.GetRestClient();

            var anyRequest = new RestRequest
            {
                Method = Method.GET,
                Resource = AnyResource,
                RootElement = AnyRootElement
            };

            var successfulResponseMessage = CreateSuccessfulResponseMessage();
            tc.HttpClient.GetAsync(AnyRequestUri + anyRequest.Resource, CancellationToken.None)
                .Returns(Task.FromResult(successfulResponseMessage));

            var response =
                await client.ExecuteRequestAsync<AnyResponse>(anyRequest, AnyRequestUri, CancellationToken.None);

            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
            Assert.AreEqual(AnyStringField, response.Data.AnyStringField);
            Assert.AreEqual(AnyIntField, response.Data.AnyIntField);
            Assert.IsNull(response.Exception);
            var unused = tc.HttpClient.Received(1).GetAsync(AnyRequestUri + anyRequest.Resource, CancellationToken.None);
        }

        [TestMethod]
        public async Task RestClientPostRequestReturnsResponse()
        {
            var tc = new TestContext();

            var client = tc.GetRestClient();

            var anyRequest = new RestRequest
            {
                Method = Method.POST,
                Resource = AnyResource,
                RootElement = AnyRootElement
            };

            anyRequest.AddBody(AnyPostContent);

            var successfulResponseMessage = CreateSuccessfulResponseMessage();
            tc.HttpClient.PostAsync(AnyRequestUri + anyRequest.Resource,
                Arg.Is<StringContent>(
                    stringContent =>
                        stringContent.Headers.ContentType.MediaType == MediaType &&
                        stringContent.ReadAsStringAsync().Result == AnyPostContent),
                CancellationToken.None)
                .Returns(Task.FromResult(successfulResponseMessage));

            var response =
                await client.ExecuteRequestAsync<AnyResponse>(anyRequest, AnyRequestUri, CancellationToken.None);

            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
            Assert.AreEqual(AnyStringField, response.Data.AnyStringField);
            Assert.AreEqual(AnyIntField, response.Data.AnyIntField);
            Assert.IsNull(response.Exception);
            var unused = tc.HttpClient.Received(1).PostAsync(AnyRequestUri + anyRequest.Resource,
                Arg.Is<StringContent>(
                    stringContent =>
                        stringContent.Headers.ContentType.MediaType == MediaType &&
                        stringContent.ReadAsStringAsync().Result == AnyPostContent),
                CancellationToken.None);
        }

        [TestMethod]
        public async Task RestClientRequestReturnsFailureResponse()
        {
            var tc = new TestContext();

            var client = tc.GetRestClient();

            var anyRequest = new RestRequest
            {
                Method = Method.GET,
                Resource = AnyResource,
                RootElement = AnyRootElement
            };

            var successfulResponseMessage = CreateFailedResponseMessage();
            tc.HttpClient.GetAsync(AnyRequestUri + anyRequest.Resource, CancellationToken.None)
                .Returns(Task.FromResult(successfulResponseMessage));

            var response =
                await client.ExecuteRequestAsync<AnyResponse>(anyRequest, AnyRequestUri, CancellationToken.None);

            Assert.AreEqual(HttpStatusCode.InternalServerError, response.StatusCode);
            Assert.IsNull(response.Data);
            Assert.IsNotNull(response.Exception);
            Assert.IsInstanceOfType(response.Exception, typeof(HttpRequestException));
            var unused = tc.HttpClient.Received(1).GetAsync(AnyRequestUri + anyRequest.Resource, CancellationToken.None);
        }

        private HttpResponseMessage CreateSuccessfulResponseMessage()
        {
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(ExpectedReturnJson, Encoding, MediaType)
            };
        }

        private HttpResponseMessage CreateFailedResponseMessage()
        {
            return new HttpResponseMessage(HttpStatusCode.InternalServerError)
            {
                Content = new StringContent("AnyFailedContent", Encoding, MediaType)
            };
        }

        private class TestContext
        {
            public readonly IHttpClient HttpClient = Substitute.For<IHttpClient>();

            public readonly IDeserializer Deserializer = Substitute.For<IDeserializer>();

            public IRestClient GetRestClient()
            {
                var injector = TangFactory.GetTang().NewInjector();
                injector.BindVolatileInstance(GenericType<IHttpClient>.Class, HttpClient);
                ////injector.BindVolatileInstance(GenericType<IDeserializer>.Class, Deserializer);
                return injector.GetInstance<IRestClient>();
            }
        }

        private class AnyResponse
        {
            public string AnyStringField { get; set; }

            public int AnyIntField { get; set; }
        }

        private class AnyRequest
        {
            public string AnyStringField { get; set; }
        }
    }
}