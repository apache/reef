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

using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

// General Information about an assembly is controlled through the following 
// set of attributes. Change these attribute values to modify the information
// associated with an assembly.
[assembly: AssemblyTitle("Org.Apache.REEF.Common")]
[assembly: AssemblyDescription("")]
[assembly: AssemblyConfiguration("")]
[assembly: AssemblyCompany("")]
[assembly: AssemblyProduct("Org.Apache.REEF.Common")]
[assembly: AssemblyCopyright("Copyright © 2016")]
[assembly: AssemblyTrademark("")]
[assembly: AssemblyCulture("")]

// Setting ComVisible to false makes the types in this assembly not visible 
// to COM components.  If you need to access a type in this assembly from 
// COM, set the ComVisible attribute to true on that type.
[assembly: ComVisible(false)]

// The following GUID is for the ID of the typelib if this project is exposed to COM
[assembly: Guid("a810ee4a-fe13-4536-9e9c-5275b16e0842")]

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

// Common and Evaluator share APIs that should not be exposed to the user
[assembly: InternalsVisibleTo("Org.Apache.REEF.Evaluator, publickey=" +
 "00240000048000009400000006020000002400005253413100040000010001005df3e621d886a9" +
 "9c03469d0f93a9f5d45aa2c883f50cd158759e93673f759ec4657fd84cc79d2db38ef1a2d914cc" +
 "b7c717846a897e11dd22eb260a7ce2da2dccf0263ea63e2b3f7dac24f28882aa568ef544341d17" +
 "618392a1095f4049ad079d4f4f0b429bb535699155fd6a7652ec7d6c1f1ba2b560f11ef3a86b5945d288cf")]

// Common and Driver share APIs that should not be exposed to the user
[assembly: InternalsVisibleTo("Org.Apache.REEF.Driver, publickey=" +
 "00240000048000009400000006020000002400005253413100040000010001005df3e621d886a9" +
 "9c03469d0f93a9f5d45aa2c883f50cd158759e93673f759ec4657fd84cc79d2db38ef1a2d914cc" +
 "b7c717846a897e11dd22eb260a7ce2da2dccf0263ea63e2b3f7dac24f28882aa568ef544341d17" +
 "618392a1095f4049ad079d4f4f0b429bb535699155fd6a7652ec7d6c1f1ba2b560f11ef3a86b5945d288cf")]

// Common and IO share APIs that should not be exposed to the user
[assembly: InternalsVisibleTo("Org.Apache.REEF.IO, publickey=" +
 "00240000048000009400000006020000002400005253413100040000010001005df3e621d886a9" +
 "9c03469d0f93a9f5d45aa2c883f50cd158759e93673f759ec4657fd84cc79d2db38ef1a2d914cc" +
 "b7c717846a897e11dd22eb260a7ce2da2dccf0263ea63e2b3f7dac24f28882aa568ef544341d17" +
 "618392a1095f4049ad079d4f4f0b429bb535699155fd6a7652ec7d6c1f1ba2b560f11ef3a86b5945d288cf")]

// Allow the tests access to `internal` APIs
[assembly: InternalsVisibleTo("Org.Apache.REEF.Common.Tests, publickey=" +
 "00240000048000009400000006020000002400005253413100040000010001005df3e621d886a9" +
 "9c03469d0f93a9f5d45aa2c883f50cd158759e93673f759ec4657fd84cc79d2db38ef1a2d914cc" +
 "b7c717846a897e11dd22eb260a7ce2da2dccf0263ea63e2b3f7dac24f28882aa568ef544341d17" +
 "618392a1095f4049ad079d4f4f0b429bb535699155fd6a7652ec7d6c1f1ba2b560f11ef3a86b5945d288cf")]
[assembly: InternalsVisibleTo("Org.Apache.REEF.Evaluator.Tests, publickey=" +
 "00240000048000009400000006020000002400005253413100040000010001005df3e621d886a9" +
 "9c03469d0f93a9f5d45aa2c883f50cd158759e93673f759ec4657fd84cc79d2db38ef1a2d914cc" +
 "b7c717846a897e11dd22eb260a7ce2da2dccf0263ea63e2b3f7dac24f28882aa568ef544341d17" +
 "618392a1095f4049ad079d4f4f0b429bb535699155fd6a7652ec7d6c1f1ba2b560f11ef3a86b5945d288cf")]

// Allow NSubstitute to create proxy implementations
[assembly: InternalsVisibleTo("DynamicProxyGenAssembly2, PublicKey=002400000480000" +
 "0940000000602000000240000525341310004000001000100c547cac37abd99c8db225ef2f6c8a36" +
 "02f3b3606cc9891605d02baa56104f4cfc0734aa39b93bf7852f7d9266654753cc297e7d2edfe0ba" +
 "c1cdcf9f717241550e0a7b191195b7667bb4f64bcb8e2121380fd1d9d46ad2d92d2d15605093924c" +
 "ceaf74c4861eff62abf69b9291ed0a340e113be11e6a7d3113e92484cf7045cc7")]