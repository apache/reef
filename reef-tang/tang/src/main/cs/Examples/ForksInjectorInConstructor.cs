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
using Com.Microsoft.Tang.Implementations;
using Com.Microsoft.Tang.Interface;

namespace Com.Microsoft.Tang.Examples
{
    public class ForksInjectorInConstructor
    {
        [Inject]
        public ForksInjectorInConstructor(IInjector i)
        {
            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder(new string[] { @"Com.Microsoft.Tang.Examples" });
            //cb.BindImplementation(Number.class, typeof(Int32));
            i.ForkInjector(new IConfiguration[] { cb.Build() });
        }
    }
}
