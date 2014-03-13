// This is the main DLL file.

#include "InteropUtil.h"
#include "javabridge_NativeInterop.h"
#include "JavaClrBridge.h"
#include "InteropAssemblies.h"



#define _MT 1
#define _DLL 1

using namespace System;
using namespace System::IO;
using namespace System::Collections::Generic;
using namespace System::Runtime::InteropServices;
using namespace System::Reflection;
using namespace ClrHandler;

static Assembly^ MyResolveEventHandler(Object^ sender, ResolveEventArgs^ args);

static void ManagedLog (String^ fname, String^ msg)
{		
	Console::WriteLine (fname);
#ifdef xx
	try
	{
	
	FileStream^ fs = gcnew FileStream ("d:\\BondIoLog.txt", System::IO::FileMode::Append);
	StreamWriter^ sw = gcnew StreamWriter(fs);
	
	sw->WriteLine (DateTime::Now.ToString("s") + " [" + fname + "] [" + msg + "]");
	sw->Flush();
	sw->Close();
	fs->Close();
	}
	catch (Exception^ ex)
	{
		Console::WriteLine (ex->Message);
		Console::WriteLine ("ex");
	}
	catch (...)
	{
	}
#endif	
}

static void Log1 (const wchar_t* fname, const wchar_t* msg)
{
	ManagedLog (gcnew String(fname), gcnew String(msg));	
}

static void Log0 (char* fname)
{
	ManagedLog (gcnew String(fname), "");	
}


	
	static void MarshalErrorToJava (
		JNIEnv *env, 
		jobject  jerrorInfo,
		int errorNo,
		String^ exceptionString
		)
	{
		jclass objectClass;
		jfieldID fieldID;		

		objectClass = env->GetObjectClass(jerrorInfo);
		fieldID = env->GetFieldID(objectClass, "errorNo", "I");
		env->SetIntField (jerrorInfo, fieldID, errorNo);

		pin_ptr<const wchar_t> wchExceptionString = PtrToStringChars(exceptionString);		
		jstring jexceptionString = env->NewString((const jchar*)wchExceptionString, exceptionString->Length);
		fieldID = env->GetFieldID(objectClass, "exceptionString", "Ljava/lang/String;");
		env->SetObjectField(jerrorInfo, fieldID, jexceptionString);
	}

	
JNIEXPORT void JNICALL Java_javabridge_NativeInterop_loadClrAssembly
  (
	JNIEnv *env, 
	jclass  tobj, 
	jstring jfileName)
{
	Log0("In c++ JNIEXPORT void JNICALL Java_javabridge_NativeInterop_loadClrAssembly +");
	const wchar_t* charAsmName = UnicodeCppStringFromJavaString (env, jfileName);
	int len = env->GetStringLength(jfileName);	
	
	String^  asmName = Marshal::PtrToStringUni((IntPtr)(unsigned short*) charAsmName, len);	
	System::Reflection::Assembly^ asm1 = Assembly::LoadFrom(asmName);
	AssemblyUtil::Add(asm1);
}

/*
 * Class:     com_yingda_NativeInterop
 * Method:    createHandler1
 * Signature: (Ljava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_javabridge_NativeInterop_createHandler1
 (
	JNIEnv *env, 
	jclass  tobj, 
	jstring jstrConfig)
{
	try 
	{
	
	const wchar_t* charConfig = UnicodeCppStringFromJavaString (env, jstrConfig);
	int lenConfig = env->GetStringLength(jstrConfig);		
	String^  strConfig = Marshal::PtrToStringUni((IntPtr)(unsigned short*) charConfig, lenConfig);		
	return ClrHandlerWrapper::CreateFromString_ClrHandler(strConfig);
	}
	catch (System::Exception^ ex)
	{
		Console::WriteLine("Exception in Java_javabridge_NativeInterop_createHandler1");
		Console::WriteLine(ex->Message);
		Console::WriteLine(ex->StackTrace);
	}
	return 0;
}



/*
 * Class:     com_yingda_NativeInterop
 * Method:    clrHandlerOnNext
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_javabridge_NativeInterop_clrHandlerOnNext
 (
	JNIEnv *env, 
	jclass  tobj, 
    jlong   handle,
	jbyteArray value)
{
	byte* bytes = (byte*)env->GetByteArrayElements (value, FALSE);
	int len = env->GetArrayLength(value);
	array<byte>^  bytearray = gcnew array<byte>(len);
	for (int i=0; i<len; i++)
	{
		bytearray[i] = bytes[i];
	}
	ClrHandlerWrapper::CallMethod_ClrHandler_OnNext(handle, bytearray);
}





/*
public ref class Logger : public Datalab::ILogger
{
	jclass  _jerrorInfo;
	jmethodID _jmid;
	JNIEnv* _env;

public:
	Logger (JNIEnv* env, jclass jerrInfo, jmethodID jmid)
	{
		_env = env;
		_jerrorInfo = jerrInfo;
		_jmid = jmid;
	}
	virtual void Log(String^ message, Datalab::TraceLevel traceLevel)
	{	
		pin_ptr<const wchar_t> wch = PtrToStringChars(message);		
		jstring ret = _env->NewString((const jchar*)wch, message->Length);
		_env->CallStaticVoidMethod (_jerrorInfo, _jmid, ret, traceLevel);
	}
};


JNIEXPORT jlong JNICALL Java_com_microsoft_hadoop_mapreduce_lib_aipinput_NativeInterop_createFileReader
  (JNIEnv *env, 
  jclass cls, 
			jstring jfileName, 
		    jstring  jfileStorageAccount,
		    jstring  jfileStorageKey,
		    jstring  jcontainerName,
		    jstring  jdfsHttpAddress,
			jstring  jprotectionServicesLabelsBlobUri,
			jstring  jschemaKind,
			jobject  jinteropLogger,
			jobject  jinteropReturnCode
			)
{		
	
	Log0 ("c++ cfr enter");
	jclass jcinteropLogger = env->GetObjectClass(jinteropLogger);	
	jmethodID jmid = env->GetStaticMethodID(jcinteropLogger, "Trace", "(Ljava/lang/String;I)V");	
	Logger^ logger = gcnew Logger (env, jcinteropLogger, jmid);	
	
	String^ fileName = ManagedStringFromJavaString (env, jfileName);
	String^ fileStorageAccount = ManagedStringFromJavaString (env, jfileStorageAccount);
	String^ fileStorageKey = ManagedStringFromJavaString (env, jfileStorageKey);
	String^ containerName = ManagedStringFromJavaString (env, jcontainerName);
	String^ dfsHttpAddress = ManagedStringFromJavaString (env, jdfsHttpAddress);
	String^ protectionServicesLabelsBlobUri = ManagedStringFromJavaString (env, jprotectionServicesLabelsBlobUri);
	String^ schemaKind = ManagedStringFromJavaString (env, jschemaKind);

	int errorNo = 0;
	String^ exceptionString = nullptr;
	
	unsigned long long cookie  = Datalab::BondIo::GetCookieForFile(
		fileName,
		fileStorageAccount,
		fileStorageKey,
		containerName,
		dfsHttpAddress,
		protectionServicesLabelsBlobUri,
		schemaKind,
		logger,
		errorNo,
		exceptionString
		);
	if (errorNo != 0)
	{
		MarshalErrorToJava (env, jinteropReturnCode, errorNo, exceptionString);
	}

	return cookie;
}

JNIEXPORT jstring JNICALL Java_com_microsoft_hadoop_mapreduce_lib_aipinput_NativeInterop_GetNextRow
(
	JNIEnv*		env, 
	jclass		cls, 
	jlong		cookie,
	jobject		jinteropLogger,
	jobject		jinteropReturnCode
)
{	
	ManagedLog("GetNexRow cookie", cookie.ToString());
	jclass jcinteropLogger = env->GetObjectClass(jinteropLogger);	
	jmethodID jmid = env->GetStaticMethodID(jcinteropLogger, "Trace", "(Ljava/lang/String;I)V");	
	Logger^ logger = gcnew Logger (env, jcinteropLogger, jmid);	

	int errorNo;
	String^ exceptionString = nullptr;

	String^ str = Datalab::BondIo::GetNextRow (
		cookie,
		logger,
		errorNo,
		exceptionString
		);

	if (errorNo != 0)
	{
		MarshalErrorToJava (env, jinteropReturnCode, errorNo, exceptionString);
	}
	
	jstring ret = NULL;
	
	if (!String::IsNullOrEmpty(str))
	{	
		ManagedLog("GetNexRow not null", str);
		pin_ptr<const wchar_t> wch = PtrToStringChars(str);
		Log1(L"Unmanaged GetNexRow not null", wch);
		ret = env->NewString((const jchar*)wch, str->Length);
		if (ret == NULL)
		{
			Log1(L"Ret is NULL", L"xx");
		}	
	}
	else 
	{
		ManagedLog("GetNexRow null", "x");
	}
	return ret;
}
*/
