#include "InteropUtil.h"

using namespace System::Runtime::InteropServices;

const wchar_t* UnicodeCppStringFromJavaString (
	JNIEnv *env,
	jstring javaString)
{
	const wchar_t* cppString = NULL;
	if (NULL != javaString)
	{
		cppString = (const wchar_t* )env->GetStringChars( javaString, 0);
	}
	return cppString;
}

void ReleaseUnicodeCppString (
	JNIEnv*			env,
	jstring			javaString,
	jchar*			cppString)
{	
	if (NULL != cppString)
	{
		env->ReleaseStringChars(javaString, (jchar *)cppString);
	}	
}

String^ ManagedStringFromJavaString (
	JNIEnv *env,
	jstring javaString)
{	
	int len = env->GetStringLength(javaString);
	const wchar_t* wcsStr = UnicodeCppStringFromJavaString (env, javaString);
	String^ managedStr = (NULL == wcsStr || 0 == len) ? nullptr : Marshal::PtrToStringUni((IntPtr)(unsigned short*)wcsStr, len);
	ReleaseUnicodeCppString (env, javaString, (jchar*)wcsStr);
	return managedStr;
}

jstring JavaStringFromManagedString(
	JNIEnv *env,
	String^ managedString)
{	
	pin_ptr<const wchar_t> wch = PtrToStringChars(managedString);		
	return env->NewString((const jchar*)wch, managedString->Length);
}

array<byte>^ ManagedByteArrayFromJavaByteArray(
	JNIEnv *env,
	jbyteArray javaByteArray)
{
	byte* bytes = (byte*)env->GetByteArrayElements (javaByteArray, FALSE);
	int len = env->GetArrayLength(javaByteArray);
	array<byte>^  managedByteArray = gcnew array<byte>(len);
	//System::Array
	for (int i=0; i<len; i++)
	{
		managedByteArray[i] = bytes[i];
	}
	return managedByteArray;
}

jbyteArray JavaByteArrayFromManagedByteArray(
	JNIEnv *env,
	array<byte>^ managedByteArray)
{	
	jbyteArray javaByteArray = env->NewByteArray(managedByteArray->Length);
	pin_ptr<Byte> p = &managedByteArray[0];
	env->SetByteArrayRegion(javaByteArray, 0, managedByteArray->Length, (jbyte*) p);
	return javaByteArray;
}

jlongArray JavaLongArrayFromManagedLongArray(
	JNIEnv *env,
	array<unsigned long long>^ managedLongArray)
{	
	jlongArray javaLongArray = env->NewLongArray(managedLongArray->Length);
	pin_ptr<unsigned long long> p = &managedLongArray[0];
	env->SetLongArrayRegion(javaLongArray, 0, managedLongArray->Length, (jlong*) p);
	return javaLongArray;
}

JNIEnv* RetrieveEnv(JavaVM* jvm)
{
	JNIEnv *env;
	if (jvm->AttachCurrentThread((void **) &env, NULL) != 0) {
		fprintf(stdout, "cannot attach jni env to current jvm thread.\n"); fflush (stdout);
		throw;
    }
	return env;
}