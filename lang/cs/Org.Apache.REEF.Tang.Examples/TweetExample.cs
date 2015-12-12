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

namespace Org.Apache.REEF.Tang.Examples
{
    public interface ISMS
    {
        void SendSMS(string msg, long phoneNumber);
    }

    public interface ITweetFactory
    {
        string GetTweet();
    } 

    public class MockTweetFactory : ITweetFactory 
    {
        [Inject]
        public MockTweetFactory() 
        {
        }

        public string GetTweet() 
        {
            return "@tw #bbq bbqftw!!! gopher://vuwed.wefd/bbqftw!";
        }
    }

    public class MockSMS : ISMS 
    {
        [Inject]
        public MockSMS() 
        {
        }

        public void SendSMS(string msg, long phoneNumber) 
        {
            if (phoneNumber != 8675309) 
            {
                throw new ArgumentException("Unknown recipient");
            }

            // success!
        }
    }

    public class Tweeter
    {
        readonly ITweetFactory tw;
        readonly ISMS sms;
        readonly long phoneNumber;

        [NamedParameter(Documentation = "Phone number", ShortName = "number", DefaultValue = "1800")]
        public class PhoneNumber : Name<long>
        { 
        }
        [Inject]
        public Tweeter(ITweetFactory tw, ISMS sms, [Parameter(typeof(PhoneNumber))] long phoneNumber)
        {
            this.tw = tw;
            this.sms = sms;
            this.phoneNumber = phoneNumber;
        }

        [Inject]
        public Tweeter([Parameter(typeof(PhoneNumber))] long phoneNumber)
        {
            this.phoneNumber = phoneNumber;
        }

        public void sendMessage()
        {
            sms.SendSMS(tw.GetTweet(), phoneNumber);
        }
    }
}
