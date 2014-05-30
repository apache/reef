#include "InteropUtil.h"
#include "com_microsoft_reef_javabridge_NativeInterop.h"
#include "JavaClrBridge.h"
#include "InteropAssemblies.h"
#include "InteropReturnInfo.h"
#include "Clr2JavaImpl.h"
#include "InteropLogger.h"
#include "BinaryUtil.h"
#include "malloc.h"

using namespace System;
using namespace System::IO;
using namespace System::Collections::Generic;
using namespace System::Runtime::InteropServices;
using namespace System::Reflection;
using namespace Microsoft::Reef::Driver::Bridge;


static void ManagedLog (String^ fname, String^ msg)
{		
	Console::WriteLine (fname);
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

	
JNIEXPORT void JNICALL Java_com_microsoft_reef_javabridge_NativeInterop_loadClrAssembly
  (
	JNIEnv *env, 
	jclass  tobj, 
	jstring jfileName)
{
	try
	{
		Console::Write("+Java_com_microsoft_reef_javabridge_NativeInterop_loadClrAssembly: ");
		const wchar_t* charAsmName = UnicodeCppStringFromJavaString (env, jfileName);
		int len = env->GetStringLength(jfileName);	
		wchar_t* fileName = (wchar_t* )_alloca((len+2)* sizeof(wchar_t));
		memcpy(fileName, charAsmName, (len+2)* sizeof(wchar_t));
		fileName[len] = 0;
		String^ asmName = ManagedStringFromJavaString(env, jfileName);
		Console::WriteLine(asmName);

		BINARY_TYPE binaryType = IsManagedBinary(fileName);
		if (binaryType == BINARY_TYPE_CLR)
		{
			System::Reflection::Assembly^ asm1 = Assembly::LoadFrom(asmName);
			AssemblyUtil::Add(asm1);
		}
		else if (binaryType == BINARY_TYPE_NATIVE)
		{
			HANDLE handle = LoadLibraryW(fileName);
		}
	}
	catch (System::Exception^ ex)
	{
		// We do not propagate the exception back to Java to stop driver here 
		// since failure to load an assembly is not necesary devastating
		Console::WriteLine("Exceptions in Java_com_microsoft_reef_javabridge_NativeInterop_loadClrAssembly");
		Console::WriteLine(ex->Message);
		Console::WriteLine(ex->StackTrace);
	}

}

/*
 * Class:     com_microsoft_reef_javabridge_NativeInterop
 * Method:    CallClrSystemOnStartHandler
 * Signature: (Ljava/lang/String;)V
 */
JNIEXPORT jlongArray JNICALL Java_com_microsoft_reef_javabridge_NativeInterop_CallClrSystemOnStartHandler
  (JNIEnv * env, jclass jclassx, jstring dateTimeString)
{
	try
	{
		Console::WriteLine("+Java_com_microsoft_reef_javabridge_NativeInterop_CallClrSystemOnStartHandler");
		const wchar_t* charConfig = UnicodeCppStringFromJavaString (env, dateTimeString);
		int lenConfig = env->GetStringLength(dateTimeString);		
		String^  strConfig = Marshal::PtrToStringUni((IntPtr)(unsigned short*) charConfig, lenConfig);		
		DateTime dt = DateTime::Now; 
		array<unsigned long long>^ handlers = ClrSystemHandlerWrapper::Call_ClrSystemStartHandler_OnStart(dt);
		return JavaLongArrayFromManagedLongArray(env, handlers);
	}
	catch (System::Exception^ ex)
	{
		// we cannot get error back to java here since we don't have an object to call back (although we idealy should...)
		Console::WriteLine("Exceptions in Java_com_microsoft_reef_javabridge_NativeInterop_CallClrSystemOnStartHandler");
		Console::WriteLine(ex->Message);
		Console::WriteLine(ex->StackTrace);
	}
}

/*
 * Class:     com_microsoft_reef_javabridge_NativeInterop
 * Method:    ClrSystemAllocatedEvaluatorHandlerOnNext
 * Signature: (JLcom.microsoft.reef.javabridge/AllocatedEvaluatorBridge;Lcom.microsoft.reef.javabridge/InteropLogger;)V
 */
JNIEXPORT void JNICALL Java_com_microsoft_reef_javabridge_NativeInterop_ClrSystemAllocatedEvaluatorHandlerOnNext
  (JNIEnv *env, jclass cls, jlong handle, jobject jallocatedEvaluatorBridge, jobject jlogger)
{
	Console::WriteLine("+Java_com_microsoft_reef_javabridge_NativeInterop_ClrSystemAllocatedEvaluatorHandlerOnNext");
	AllocatedEvaluatorClr2Java^ allocatedEval = gcnew AllocatedEvaluatorClr2Java(env, jallocatedEvaluatorBridge);
	try{		
		ClrSystemHandlerWrapper::Call_ClrSystemAllocatedEvaluatorHandler_OnNext(handle, allocatedEval);
	}
	catch (System::Exception^ ex)
	{
		String^ errorMessage = "Exception in Call_ClrSystemAllocatedEvaluatorHandler_OnNext" + ex -> Message + ex -> StackTrace;
		Console::WriteLine(errorMessage);
		allocatedEval -> OnError(errorMessage);
	}
}

/*
 * Class:     com_microsoft_reef_javabridge_NativeInterop
 * Method:    ClrSystemActiveContextHandlerOnNext
 * Signature: (JLcom.microsoft.reef.javabridge/ActiveContextBridge;Lcom.microsoft.reef.javabridge/InteropLogger;)V
 */
JNIEXPORT void JNICALL Java_com_microsoft_reef_javabridge_NativeInterop_ClrSystemActiveContextHandlerOnNext
  (JNIEnv *env, jclass cls, jlong handle, jobject jactiveContextBridge, jobject jlogger)
{
	Console::WriteLine("+Java_com_microsoft_reef_javabridge_NativeInterop_ClrSystemActiveContextHandlerOnNext");
	ActiveContextClr2Java^ activeContextBrdige = gcnew ActiveContextClr2Java(env, jactiveContextBridge);
	try{	
		ClrSystemHandlerWrapper::Call_ClrSystemActiveContextHandler_OnNext(handle, activeContextBrdige);
	}
	catch (System::Exception^ ex)
	{
		String^ errorMessage = "Exception in Call_ClrSystemActiveContextHandler_OnNext" + ex -> Message + ex -> StackTrace;
		Console::WriteLine(errorMessage);
		activeContextBrdige -> OnError(errorMessage);
	}
}

/*
 * Class:     com_microsoft_reef_javabridge_NativeInterop
 * Method:    ClrSystemEvaluatorRequstorHandlerOnNext
 * Signature: (JLcom.microsoft.reef.javabridge/EvaluatorRequstorBridge;Lcom.microsoft.reef.javabridge/InteropLogger;)V
 */
JNIEXPORT void JNICALL Java_com_microsoft_reef_javabridge_NativeInterop_ClrSystemEvaluatorRequstorHandlerOnNext
  (JNIEnv *env, jclass cls, jlong handle, jobject jevaluatorRequestorBridge, jobject jlogger)
{
	Console::WriteLine("+Java_com_microsoft_reef_javabridge_NativeInterop_ClrSystemEvaluatorRequstorHandlerOnNext");
	EvaluatorRequestorClr2Java^ evaluatorRequestorBridge = gcnew EvaluatorRequestorClr2Java(env, jevaluatorRequestorBridge);
	try{		
		ClrSystemHandlerWrapper::Call_ClrSystemEvaluatorRequestor_OnNext(handle, evaluatorRequestorBridge);
	}
	catch (System::Exception^ ex)
	{
		String^ errorMessage = "Exception in Call_ClrSystemEvaluatorRequestor_OnNext" + ex -> Message + ex -> StackTrace;
		Console::WriteLine(errorMessage);
		evaluatorRequestorBridge -> OnError(errorMessage);
	}
}

/*
 * Class:     com_microsoft_reef_javabridge_NativeInterop
 * Method:    ClrSystemTaskMessageHandlerOnNext
 * Signature: (J[BLcom.microsoft.reef.javabridge/TaskMessageBridge;Lcom.microsoft.reef.javabridge/InteropLogger;)V
 */
JNIEXPORT void JNICALL Java_com_microsoft_reef_javabridge_NativeInterop_ClrSystemTaskMessageHandlerOnNext
	(JNIEnv *env, jclass cls, jlong handle, jbyteArray jmessage, jobject jtaskMessageBridge, jobject jlogger)
{
	Console::WriteLine("+Java_com_microsoft_reef_javabridge_NativeInterop_ClrSystemTaskMessageHandlerOnNext");
	TaskMessageClr2Java^ taskMesageBridge = gcnew TaskMessageClr2Java(env, jtaskMessageBridge);
	array<byte>^ message = ManagedByteArrayFromJavaByteArray(env, jmessage);
	try{		
		ClrSystemHandlerWrapper::Call_ClrSystemTaskMessage_OnNext(handle, taskMesageBridge, message);
	}
	catch (System::Exception^ ex)
	{
		String^ errorMessage = "Exception in Call_ClrSystemTaskMessage_OnNext" + ex -> Message + ex -> StackTrace;
		Console::WriteLine(errorMessage);
		taskMesageBridge -> OnError(errorMessage);
	}
}

/*
 * Class:     com_microsoft_reef_javabridge_NativeInterop
 * Method:    ClrSysteFailedTaskHandlerOnNext
 * Signature: (JLcom.microsoft.reef.javabridge/FailedTaskBridge;Lcom.microsoft.reef.javabridge/InteropLogger;)V
 */
JNIEXPORT void JNICALL Java_com_microsoft_reef_javabridge_NativeInterop_ClrSystemFailedTaskHandlerOnNext
  (JNIEnv *env , jclass cls, jlong handler, jobject jfailedTask, jobject jlogger)
{
	Console::WriteLine("+Java_com_microsoft_reef_javabridge_NativeInterop_ClrSystemFailedTaskHandlerOnNext");
	FailedTaskClr2Java^ failedTaskBridge = gcnew FailedTaskClr2Java(env, jfailedTask);
	try{		
		ClrSystemHandlerWrapper::Call_ClrSystemFailedTask_OnNext(handler, failedTaskBridge);
	}
	catch (System::Exception^ ex)
	{
		String^ errorMessage = "Exception in Call_ClrSystemTaskMessage_OnNext" + ex -> Message + ex -> StackTrace;
		Console::WriteLine(errorMessage);
		failedTaskBridge -> OnError(errorMessage);
	}
}

/*
 * Class:     com_microsoft_reef_javabridge_NativeInterop
 * Method:    ClrSystemFailedEvaluatorHandlerOnNext
 * Signature: (JLcom.microsoft.reef.javabridge/FailedEvaluatorBridge;Lcom.microsoft.reef.javabridge/InteropLogger;)V
 */
JNIEXPORT void JNICALL Java_com_microsoft_reef_javabridge_NativeInterop_ClrSystemFailedEvaluatorHandlerOnNext
  (JNIEnv *env , jclass cls, jlong handler, jobject jfailedEvaluator, jobject jlogger)
{
	Console::WriteLine("+Java_com_microsoft_reef_javabridge_NativeInterop_ClrSystemFailedEvaluatorHandlerOnNext");
	FailedEvaluatorClr2Java^ failedEvaluatorBridge = gcnew FailedEvaluatorClr2Java(env, jfailedEvaluator);
	try{
		ClrSystemHandlerWrapper::Call_ClrSystemFailedEvaluator_OnNext(handler, failedEvaluatorBridge);
	}
	catch (System::Exception^ ex)
	{
		String^ errorMessage = "Exception in Call_ClrSystemFailedEvaluator_OnNext" + ex -> Message + ex -> StackTrace;
		Console::WriteLine(errorMessage);
		failedEvaluatorBridge -> OnError(errorMessage);
	}
}

/*
 * Class:     com_microsoft_reef_javabridge_NativeInterop
 * Method:    ClrSystemHttpServerEventHandlerOnHttpRequest
 * Signature: (JLcom.microsoft.reef.javabridge/HttpServerEventBridge;Lcom.microsoft.reef.javabridge/InteropLogger;)V
 */
JNIEXPORT void JNICALL Java_com_microsoft_reef_javabridge_NativeInterop_ClrSystemHttpServerHandlerOnNext
  (JNIEnv *env , jclass cls, jlong handler, jobject jhttpServerEventBridge, jobject jlogger)
 {
	Console::WriteLine("+Java_com_microsoft_reef_javabridge_NativeInterop_ClrSystemHttpServerHandlerOnNext");
	HttpServerClr2Java^ httpServerClr2Java = gcnew HttpServerClr2Java(env, jhttpServerEventBridge);	
	 try{
		ClrSystemHandlerWrapper::Call_ClrSystemHttpServer_OnNext(handler, httpServerClr2Java);
	}
	catch (System::Exception^ ex)
	{
		String^ errorMessage = "Exception in Call_ClrSystemHttpServer_OnNext" + ex -> Message + ex -> StackTrace;
		Console::WriteLine(errorMessage);
		httpServerClr2Java -> OnError(errorMessage);
	}
}

/*
 * Class:     com_microsoft_reef_javabridge_NativeInterop
 * Method:    ClrSystemCompletedTaskHandlerOnNext
 * Signature: (JLcom.microsoft.reef.javabridge/CompletedTaskBridge;Lcom.microsoft.reef.javabridge/InteropLogger;)V
 */
JNIEXPORT void JNICALL Java_com_microsoft_reef_javabridge_NativeInterop_ClrSystemCompletedTaskHandlerOnNext
  (JNIEnv *env , jclass cls, jlong handler, jobject jcompletedTask, jobject jlogger)
{
	Console::WriteLine("+Java_com_microsoft_reef_javabridge_NativeInterop_ClrSystemCompletedTaskHandlerOnNext");
	CompletedTaskClr2Java^ completedTaskBridge = gcnew CompletedTaskClr2Java(env, jcompletedTask);
	try{
		ClrSystemHandlerWrapper::Call_ClrSystemCompletedTask_OnNext(handler, completedTaskBridge);
	}
	catch (System::Exception^ ex)
	{
		String^ errorMessage = "Exception in Call_ClrSystemCompletedTask_OnNext" + ex -> Message + ex -> StackTrace;
		Console::WriteLine(errorMessage);
		completedTaskBridge -> OnError(errorMessage);
	}
}

/*
 * Class:     com_microsoft_reef_javabridge_NativeInterop
 * Method:    ClrBufferedLog
 * Signature: (ILjava/lang/String;)V
 */
JNIEXPORT void JNICALL Java_com_microsoft_reef_javabridge_NativeInterop_ClrBufferedLog
  (JNIEnv *env, jclass cls, jint logLevel, jstring message)
{
    try {
        if (!JavaClrBridge::LoggerWrapper::initialized) {
            Console::WriteLine("Initializing CLRBufferedLogHandler in java bridge...");
            JavaClrBridge::LoggerWrapper::logger->Listeners->Add(gcnew System::Diagnostics::ConsoleTraceListener());
            JavaClrBridge::LoggerWrapper::initialized = true;
        }
        
        System::Diagnostics::TraceEventType eventType;
        switch (logLevel) {
            case 0: eventType = System::Diagnostics::TraceEventType::Stop; break;
            case 1: eventType = System::Diagnostics::TraceEventType::Error; break;
            case 2: eventType = System::Diagnostics::TraceEventType::Warning; break;
            case 3: eventType = System::Diagnostics::TraceEventType::Information; break;
            case 4: eventType = System::Diagnostics::TraceEventType::Verbose; break;
            default: throw gcnew System::Exception("Exception in Java_javabridge_nativeInterop_ClrBufferedLog: Log level does not exist");
        }

        String^ msg = ManagedStringFromJavaString(env, message);
        msg = System::String::Concat(System::DateTime::Now, msg);
        JavaClrBridge::LoggerWrapper::logger->TraceEvent(eventType, 0, msg);
    }
    catch (System::Exception^ ex) {
        Console::WriteLine("Exception in Java_javabridge_NativeInterop_ClrBufferedLog");
        Console::WriteLine(ex->Message);
        Console::WriteLine(ex->StackTrace);
    }
}


