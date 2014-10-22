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
﻿
using Com.Microsoft.Tang.Annotations;

namespace Com.Microsoft.Tang.Examples
{
    public interface IActivity
    {
        byte[] Call(byte[] memento);
    }

    public sealed class HelloActivity : IActivity
    {
        [Inject]
        public HelloActivity()
        {
        }

        public byte[] Call(byte[] memento)
        {
            System.Console.WriteLine("Hello, REEF!");
            return null;
        }
    }

}
