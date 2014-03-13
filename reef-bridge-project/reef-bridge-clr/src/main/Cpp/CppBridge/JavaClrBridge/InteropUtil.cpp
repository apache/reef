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

