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
	JNIEnv*			env,
	jstring			javaString,
	jchar*			cppString);

String^ ManagedStringFromJavaString (
	JNIEnv *env,
	jstring javaString);

jstring JavaStringFromManagedString(
	JNIEnv *env,
	String^ managedString);

array<byte>^ ManagedByteArrayFromJavaByteArray(
	JNIEnv *env,
	jbyteArray javaByteArray);

jbyteArray JavaByteArrayFromManagedByteArray(
	JNIEnv *env,
	array<byte>^ managedByteArray);

jlongArray JavaLongArrayFromManagedLongArray(
	JNIEnv *env,
	array<unsigned long long>^ managedLongArray);

JNIEnv* RetrieveEnv(JavaVM* jvm);
