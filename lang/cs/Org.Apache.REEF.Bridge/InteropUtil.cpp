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

#include "InteropUtil.h"
#include "Clr2JavaImpl.h"

using namespace System::Runtime::InteropServices;

ref class ManagedLog {
  internal:
    static BridgeLogger^ LOGGER = BridgeLogger::GetLogger("<C++>InteropUtil");
};

const wchar_t* UnicodeCppStringFromJavaString (
  JNIEnv *env,
  jstring javaString) {
  const wchar_t* cppString = NULL;
  if (NULL != javaString) {
    cppString = (const wchar_t* )env->GetStringChars( javaString, 0);
  }
  return cppString;
}

void ReleaseUnicodeCppString (
  JNIEnv*     env,
  jstring     javaString,
  jchar*      cppString) {
  if (NULL != cppString) {
    env->ReleaseStringChars(javaString, (jchar *)cppString);
  }
}

String^ ManagedStringFromJavaString (
  JNIEnv *env,
  jstring javaString) {
  if (javaString != NULL) {
    int len = env->GetStringLength(javaString);
    const wchar_t* wcsStr = UnicodeCppStringFromJavaString (env, javaString);
    String^ managedStr = (NULL == wcsStr || 0 == len) ? nullptr : Marshal::PtrToStringUni((IntPtr)(unsigned short*)wcsStr, len);
    ReleaseUnicodeCppString (env, javaString, (jchar*)wcsStr);
    return managedStr;
  }
  return nullptr;
}

bool ClrBoolFromJavaBoolean(
  JNIEnv * env,
  jboolean jbool) {
  return jbool != JNI_FALSE;
}

jstring JavaStringFromManagedString(
  JNIEnv *env,
  String^ managedString) {
  pin_ptr<const wchar_t> wch = PtrToStringChars(managedString);
  return env->NewString((const jchar*)wch, managedString->Length);
}

void HandleClr2JavaError(
  JNIEnv *env,
  String^ errorMessage,
  jobject javaObject) {
  ManagedLog::LOGGER->LogStart("InteropUtil::HandleClr2JavaError");

  jclass javaClass = env->GetObjectClass (javaObject);
  jmethodID jmidOnError = env->GetMethodID(javaClass, "onError", "(Ljava/lang/String;)V");

  if (jmidOnError == NULL) {
    ManagedLog::LOGGER->Log("jmidOnError is NULL");
    return;
  }
  env -> CallObjectMethod(
    javaObject,
    jmidOnError,
    JavaStringFromManagedString(env, errorMessage));
  ManagedLog::LOGGER->LogStop("InteropUtil::HandleClr2JavaError");
}

array<byte>^ ManagedByteArrayFromJavaByteArray(
  JNIEnv *env,
  jbyteArray javaByteArray) {
  if (javaByteArray != NULL) {
    byte* bytes = (byte*)env->GetByteArrayElements (javaByteArray, FALSE);
    int len = env->GetArrayLength(javaByteArray);
    array<byte>^  managedByteArray = gcnew array<byte>(len);
    //System::Array
    for (int i = 0; i < len; i++) {
      managedByteArray[i] = bytes[i];
    }
    env->ReleaseByteArrayElements(javaByteArray, (jbyte*)bytes, JNI_ABORT);
    return managedByteArray;
  }
  return nullptr;
}

jbyteArray JavaByteArrayFromManagedByteArray(
  JNIEnv *env,
  array<byte>^ managedByteArray) {
  if (managedByteArray != nullptr) {
    jbyteArray javaByteArray = env->NewByteArray(managedByteArray->Length);
    pin_ptr<Byte> p = &managedByteArray[0];
    env->SetByteArrayRegion(javaByteArray, 0, managedByteArray->Length, (jbyte*)p);
    return javaByteArray;
  }

  return NULL;
}

JNIEnv* RetrieveEnv(JavaVM* jvm) {
  JNIEnv *env;
  if (jvm->AttachCurrentThread((void **) &env, NULL) != 0) {
    ManagedLog::LOGGER->Log("cannot attach jni env to current jvm thread.");
    throw;
  }
  return env;
}

String^ FormatJavaExceptionMessage(String^ errorMessage, Exception^ exception) {
  return String::Concat(errorMessage, Environment::NewLine, exception->StackTrace);
}
