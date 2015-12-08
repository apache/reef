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

using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

// General Information about an assembly is controlled through the following 
// set of attributes. Change these attribute values to modify the information
// associated with an assembly.
[assembly: AssemblyTitle("Evaluator")]
[assembly: AssemblyDescription("")]
[assembly: AssemblyConfiguration("")]
[assembly: AssemblyCompany("")]
[assembly: AssemblyProduct("Evaluator")]
[assembly: AssemblyCopyright("Copyright ©  2015")]
[assembly: AssemblyTrademark("")]
[assembly: AssemblyCulture("")]

// Setting ComVisible to false makes the types in this assembly not visible 
// to COM components.  If you need to access a type in this assembly from 
// COM, set the ComVisible attribute to true on that type.
[assembly: ComVisible(false)]

// The following GUID is for the ID of the typelib if this project is exposed to COM
[assembly: Guid("a64dc535-9b1e-41a4-8303-117f8b28c8c0")]

// Version information for an assembly consists of the following four values:
//
//      Major Version
//      Minor Version 
//      Build Number
//      Revision
//
// You can specify all the values or you can default the Build and Revision Numbers 
// by using the '*' as shown below:
// [assembly: AssemblyVersion("1.0.*")]
[assembly: AssemblyVersion("0.14.0.0")]
[assembly: AssemblyFileVersion("0.14.0.0")]

// Common and Evaluator share functions that should not be exposed to the user
[assembly: InternalsVisibleTo("Org.Apache.REEF.Common, publickey=" +
 "00240000048000009400000006020000002400005253413100040000010001005df3e621d886a9" +
 "9c03469d0f93a9f5d45aa2c883f50cd158759e93673f759ec4657fd84cc79d2db38ef1a2d914cc" +
 "b7c717846a897e11dd22eb260a7ce2da2dccf0263ea63e2b3f7dac24f28882aa568ef544341d17" +
 "618392a1095f4049ad079d4f4f0b429bb535699155fd6a7652ec7d6c1f1ba2b560f11ef3a86b5945d288cf")]

// Allow the tests access to `internal` APIs
[assembly: InternalsVisibleTo("Org.Apache.REEF.Tang.Tests, publickey=" +
 "00240000048000009400000006020000002400005253413100040000010001005df3e621d886a9" +
 "9c03469d0f93a9f5d45aa2c883f50cd158759e93673f759ec4657fd84cc79d2db38ef1a2d914cc" +
 "b7c717846a897e11dd22eb260a7ce2da2dccf0263ea63e2b3f7dac24f28882aa568ef544341d17" +
 "618392a1095f4049ad079d4f4f0b429bb535699155fd6a7652ec7d6c1f1ba2b560f11ef3a86b5945d288cf")]
[assembly: InternalsVisibleTo("Org.Apache.REEF.Evaluator.Tests, publickey=" +
 "00240000048000009400000006020000002400005253413100040000010001005df3e621d886a9" +
 "9c03469d0f93a9f5d45aa2c883f50cd158759e93673f759ec4657fd84cc79d2db38ef1a2d914cc" +
 "b7c717846a897e11dd22eb260a7ce2da2dccf0263ea63e2b3f7dac24f28882aa568ef544341d17" +
 "618392a1095f4049ad079d4f4f0b429bb535699155fd6a7652ec7d6c1f1ba2b560f11ef3a86b5945d288cf")]