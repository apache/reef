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

using System.Runtime.Serialization;
using Org.Apache.REEF.Driver.Bridge.Clr2java;

namespace Org.Apache.REEF.Driver.Bridge
{
    [DataContract]
    internal sealed class HttpMessage : IHttpMessage
    {
        public HttpMessage(IHttpServerBridgeClr2Java httpServerBridgeClr2Java)
        {
            HttpServerBridgeClr2Java = httpServerBridgeClr2Java;
        }

        [DataMember]
        private IHttpServerBridgeClr2Java HttpServerBridgeClr2Java { get; set; }

        public string GetRequestString()
        {
            return HttpServerBridgeClr2Java.GetQueryString();
        }

        public void SetQueryResult(string responseString)
        {
            HttpServerBridgeClr2Java.SetQueryResult(responseString);
        }

        public byte[] GetQueryReuestData()
        {
            return HttpServerBridgeClr2Java.GetQueryRequestData();            
        }

        public void SetQueryResponseData(byte[] responseData)
        {
            HttpServerBridgeClr2Java.SetQueryResponseData(responseData);
        }

        public void SetUriSpecification(string uriSpecification)
        {
            HttpServerBridgeClr2Java.SetUriSpecification(uriSpecification);
        }
    }
}
