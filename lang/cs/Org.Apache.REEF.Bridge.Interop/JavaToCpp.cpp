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
// under the License


#include <jni.h>
#include <org_apache_reef_javabridge_JavaToCppInterop.h>
#include "BridgeInteropLogger.h"

using namespace std;
using namespace Org::Apache::REEF::Driver::Bridge;

void setupCppToJava(JNIEnv * env)
{
    jclass cls = env->FindClass("CppToJavaInterop");
    jmethodID id = env->GetStaticMethodID(cls, "callback", "(J)V");
    env->CallStaticVoidMethod(cls, id);
}

JNIEXPORT void JNICALL Java_org_apache_reef_javabridge_JavaToCppInterop_initializeInterop(JNIEnv* env, jclass jObj)
{
    BridgeInteropLogger*  loggerPtr = new BridgeInteropLogger(wstring(L"JavaToC++"));
    loggerPtr->LogStart(L"THIS IS A TEST LOG START MESSAGE");
    loggerPtr->Log(L"THIS IS A TEST LOG MESSAGE");
    loggerPtr->LogStop(L"THIS IS A TEST LOG STOP MESSAGE");
    loggerPtr->LogError(L"THIS IS A TEST LOG ERROR MESSAGE", L"THIS IS THE EXCEPTION");

    setupCppToJava(env);
}

static JNINativeMethod methods[] = {
  { (char*)"initializeInterop", (char*)"(Ljava/lang/String;)V", (void*)&Java_org_apache_reef_javabridge_JavaToCppInterop_initializeInterop }
};