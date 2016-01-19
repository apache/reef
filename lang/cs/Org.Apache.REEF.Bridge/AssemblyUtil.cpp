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

#include "InteropAssemblies.h"


void AssemblyUtil::Add(Assembly^  myasm) {
  if (0 == _asmCount) {
    //asm1 = myasm;
    AppDomain^ currentDomain = AppDomain::CurrentDomain;
    currentDomain->AssemblyResolve += gcnew ResolveEventHandler(&MyResolveEventHandler);
  }
  String^ asmName = myasm->FullName->ToLower();
  Assembly^ existingAsm = nullptr;
  if (!asms2->TryGetValue(asmName, existingAsm)) {
    Console::WriteLine ("AssemblyUtil:: Adding " + asmName);
    asms2->Add(asmName , myasm);
    ++_asmCount;
  }
}

Assembly^ AssemblyUtil::FindAsm (String^ myasm) {
  Assembly^ returnAsm = nullptr;
  if (!asms2->TryGetValue(myasm->ToLower(), returnAsm)) {
    Console::WriteLine ("AssemblyUtil:: FindAsm_Not_Found " + myasm->ToString());
  }
  return returnAsm;
}

Assembly^ AssemblyUtil::MyResolveEventHandler(Object^ sender, ResolveEventArgs^ args) {
  Console::WriteLine ("AssemblyUtil:: Resolving " + args->Name);
  Assembly^ myAsm = AssemblyUtil::FindAsm(args->Name);
  if (nullptr != myAsm) {
    Console::WriteLine ("AssemblyUtil:: Found " + args->Name);
  }
  return myAsm ;
}

