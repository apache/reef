/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Com.Microsoft.Tang.Annotations;

namespace Com.Microsoft.Tang.Examples
{
    public interface SMS
    {
        void SendSMS(String msg, long phoneNumber);
    }

    public interface TweetFactory
    {
        String GetTweet();
    } 

    public class MockTweetFactory : TweetFactory 
    {
        [Inject]
        public MockTweetFactory() {
        }

        public String GetTweet() 
        {
            return "@tw #bbq bbqftw!!! gopher://vuwed.wefd/bbqftw!";
        }
    }

    public class MockSMS : SMS 
    {
        [Inject]
        public MockSMS() 
        {
        }

        public void SendSMS(String msg, long phoneNumber) 
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
        TweetFactory tw;
        SMS sms;
        long phoneNumber;

        [NamedParameter("Phone number", "number", "1800")]
        class PhoneNumber : Name<long> { }
        [Inject]
        public Tweeter(TweetFactory tw, SMS sms, [Parameter(Value = typeof(PhoneNumber))] long phoneNumber)
        {
            this.tw = tw;
            this.sms = sms;
            this.phoneNumber = phoneNumber;
        }

        [Inject]
        public Tweeter([Parameter(Value = typeof(PhoneNumber))] long phoneNumber)
        {
            this.phoneNumber = phoneNumber;
        }

        public void sendMessage()
        {
            sms.SendSMS(tw.GetTweet(), phoneNumber);
        }
    }
}
