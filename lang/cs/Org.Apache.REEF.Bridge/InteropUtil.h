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

#pragma once
#define _USING_V110_SDK71_

#pragma warning( push )
#pragma warning( disable : 4793 )
#include <jni.h>
#pragma warning( pop )
#include "mscoree.h"
#include "vcclr.h"

using namespace System;

const wchar_t* UnicodeCppStringFromJavaString (
  JNIEnv *env,
  jstring javaString);

void ReleaseUnicodeCppString (
  JNIEnv*     env,
  jstring     javaString,
  jchar*      cppString);

String^ ManagedStringFromJavaString (
  JNIEnv *env,
  jstring javaString);

bool ClrBoolFromJavaBoolean(
	JNIEnv * env,
	jboolean jbool);

jstring JavaStringFromManagedString(
  JNIEnv *env,
  String^ managedString);

array<byte>^ ManagedByteArrayFromJavaByteArray(
  JNIEnv *env,
  jbyteArray javaByteArray);

jbyteArray JavaByteArrayFromManagedByteArray(
  JNIEnv *env,
  array<byte>^ managedByteArray);

JNIEnv* RetrieveEnv(JavaVM* jvm);

const int defaultRecursionDepthForExceptionFormat = 3;

String^ FormatJavaExceptionMessage(String^ errorMessage, Exception^ exception, int recursionDepth = defaultRecursionDepthForExceptionFormat);

void HandleClr2JavaError(
  JNIEnv *env,
  String^ errorMessage,
  jobject javaObject);

extern "C" {
	JNIEXPORT void JNICALL
		Java_org_apache_reef_javabridge_NativeInterop_registerNatives(JNIEnv *env, jclass cls);
}
