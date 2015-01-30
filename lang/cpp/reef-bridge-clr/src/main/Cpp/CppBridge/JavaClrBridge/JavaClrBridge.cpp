/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#include "InteropUtil.h"
#include "org_apache_reef_javabridge_NativeInterop.h"
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
using namespace Org::Apache::Reef::Driver::Bridge;

ref class ManagedLog {
  internal:
    static BridgeLogger^ LOGGER = BridgeLogger::GetLogger("<C++>");
};

static void MarshalErrorToJava (
  JNIEnv *env,
  jobject  jerrorInfo,
  int errorNo,
  String^ exceptionString
) {
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


// Loading Clr Assembly. Note that we do not use ManagerLogger in this method since the
// logger assembly needs to be loaded by this method before it can be used.
JNIEXPORT void JNICALL Java_org_apache_reef_javabridge_NativeInterop_loadClrAssembly
(
  JNIEnv *env,
  jclass  tobj,
  jstring jfileName) {
  try {
    Console::Write("+Java_org_apache_reef_javabridge_NativeInterop_loadClrAssembly: ");
    const wchar_t* charAsmName = UnicodeCppStringFromJavaString (env, jfileName);
    int len = env->GetStringLength(jfileName);
    wchar_t* fileName = (wchar_t* )_alloca((len + 2) * sizeof(wchar_t));
    memcpy(fileName, charAsmName, (len + 2)* sizeof(wchar_t));
    fileName[len] = 0;
    String^ asmName = ManagedStringFromJavaString(env, jfileName);
    Console::WriteLine("loading " + asmName);

    BINARY_TYPE binaryType = IsManagedBinary(fileName);
    if (binaryType == BINARY_TYPE_CLR) {
      System::Reflection::Assembly^ asm1 = Assembly::LoadFrom(asmName);
      AssemblyUtil::Add(asm1);
    }
    else if (binaryType == BINARY_TYPE_NATIVE) {
      HANDLE handle = LoadLibraryW(fileName);
    }
  }
  catch (System::Exception^ ex) {
    // We do not propagate the exception back to Java to stop driver here
    // since failure to load an assembly is not necesary devastating
    Console::Write("Exceptions in Java_org_apache_reef_javabridge_NativeInterop_loadClrAssembly");
    Console::Write(ex->Message);
    Console::Write(ex->StackTrace);
  }

}

/*
 * Class:     org_apache_reef_javabridge_NativeInterop
 * Method:    CallClrSystemOnStartHandler
 * Signature: (Ljava/lang/String;)V
 */
JNIEXPORT jlongArray JNICALL Java_org_apache_reef_javabridge_NativeInterop_CallClrSystemOnStartHandler
(JNIEnv * env, jclass jclassx, jstring dateTimeString, jstring httpServerPort) {
  try {
    ManagedLog::LOGGER->Log("+Java_org_apache_reef_javabridge_NativeInterop_CallClrSystemOnStartHandler");
    const wchar_t* charConfig = UnicodeCppStringFromJavaString (env, dateTimeString);
    int lenConfig = env->GetStringLength(dateTimeString);
    String^  strConfig = Marshal::PtrToStringUni((IntPtr)(unsigned short*) charConfig, lenConfig);
    DateTime dt = DateTime::Now;

	const wchar_t* charPort = UnicodeCppStringFromJavaString (env, httpServerPort);
    int lenPort = env->GetStringLength(httpServerPort);
    String^  strPort = Marshal::PtrToStringUni((IntPtr)(unsigned short*) charPort, lenPort);

    array<unsigned long long>^ handlers = ClrSystemHandlerWrapper::Call_ClrSystemStartHandler_OnStart(dt, strPort);
    return JavaLongArrayFromManagedLongArray(env, handlers);
  }
  catch (System::Exception^ ex) {
    // we cannot get error back to java here since we don't have an object to call back (although we idealy should...)
    ManagedLog::LOGGER->LogError("Exceptions in Java_org_apache_reef_javabridge_NativeInterop_CallClrSystemOnStartHandler", ex);
    return NULL;
  }
}

/*
 * Class:     org_apache_reef_javabridge_NativeInterop
 * Method:    ClrSystemAllocatedEvaluatorHandlerOnNext
 * Signature: (JLorg/apache/reef/javabridge/AllocatedEvaluatorBridge;Lorg/apache/reef/javabridge/InteropLogger;)V
 */
JNIEXPORT void JNICALL Java_org_apache_reef_javabridge_NativeInterop_ClrSystemAllocatedEvaluatorHandlerOnNext
(JNIEnv *env, jclass cls, jlong handle, jobject jallocatedEvaluatorBridge, jobject jlogger) {
  ManagedLog::LOGGER->Log("+Java_org_apache_reef_javabridge_NativeInterop_ClrSystemAllocatedEvaluatorHandlerOnNext:");
  AllocatedEvaluatorClr2Java^ allocatedEval = gcnew AllocatedEvaluatorClr2Java(env, jallocatedEvaluatorBridge);
  try {
    ClrSystemHandlerWrapper::Call_ClrSystemAllocatedEvaluatorHandler_OnNext(handle, allocatedEval);
  }
  catch (System::Exception^ ex) {
    String^ errorMessage = "Exception in Call_ClrSystemAllocatedEvaluatorHandler_OnNext";
    ManagedLog::LOGGER->LogError(errorMessage, ex);
    allocatedEval -> OnError(errorMessage);
  }
}

/*
 * Class:     org_apache_reef_javabridge_NativeInterop
 * Method:    ClrSystemActiveContextHandlerOnNext
 * Signature: (JLorg/apache/reef/javabridge/ActiveContextBridge;Lorg/apache/reef/javabridge/InteropLogger;)V
 */
JNIEXPORT void JNICALL Java_org_apache_reef_javabridge_NativeInterop_ClrSystemActiveContextHandlerOnNext
(JNIEnv *env, jclass cls, jlong handle, jobject jactiveContextBridge, jobject jlogger) {
  ManagedLog::LOGGER->Log("+Java_org_apache_reef_javabridge_NativeInterop_ClrSystemActiveContextHandlerOnNext");
  ActiveContextClr2Java^ activeContextBrdige = gcnew ActiveContextClr2Java(env, jactiveContextBridge);
  try {
    ClrSystemHandlerWrapper::Call_ClrSystemActiveContextHandler_OnNext(handle, activeContextBrdige);
  }
  catch (System::Exception^ ex) {
    String^ errorMessage = "Exception in Call_ClrSystemActiveContextHandler_OnNext";
    ManagedLog::LOGGER->LogError(errorMessage, ex);
    activeContextBrdige -> OnError(errorMessage);
  }
}

/*
 * Class:     org_apache_reef_javabridge_NativeInterop
 * Method:    ClrSystemEvaluatorRequstorHandlerOnNext
 * Signature: (JLorg/apache/reef/javabridge/EvaluatorRequstorBridge;Lorg/apache/reef/javabridge/InteropLogger;)V
 */
JNIEXPORT void JNICALL Java_org_apache_reef_javabridge_NativeInterop_ClrSystemEvaluatorRequstorHandlerOnNext
(JNIEnv *env, jclass cls, jlong handle, jobject jevaluatorRequestorBridge, jobject jlogger) {
  ManagedLog::LOGGER->Log("+Java_org_apache_reef_javabridge_NativeInterop_ClrSystemEvaluatorRequstorHandlerOnNext");
  EvaluatorRequestorClr2Java^ evaluatorRequestorBridge = gcnew EvaluatorRequestorClr2Java(env, jevaluatorRequestorBridge);
  try {
    ClrSystemHandlerWrapper::Call_ClrSystemEvaluatorRequestor_OnNext(handle, evaluatorRequestorBridge);
  }
  catch (System::Exception^ ex) {
    String^ errorMessage = "Exception in Call_ClrSystemEvaluatorRequestor_OnNext";
    ManagedLog::LOGGER->LogError(errorMessage, ex);
    evaluatorRequestorBridge -> OnError(errorMessage);
  }
}

/*
 * Class:     org_apache_reef_javabridge_NativeInterop
 * Method:    ClrSystemTaskMessageHandlerOnNext
 * Signature: (J[BLorg/apache/reef/javabridge/TaskMessageBridge;Lorg/apache/reef/javabridge/InteropLogger;)V
 */
JNIEXPORT void JNICALL Java_org_apache_reef_javabridge_NativeInterop_ClrSystemTaskMessageHandlerOnNext
(JNIEnv *env, jclass cls, jlong handle, jbyteArray jmessage, jobject jtaskMessageBridge, jobject jlogger) {
  ManagedLog::LOGGER->Log("+Java_org_apache_reef_javabridge_NativeInterop_ClrSystemTaskMessageHandlerOnNext");
  TaskMessageClr2Java^ taskMesageBridge = gcnew TaskMessageClr2Java(env, jtaskMessageBridge);
  array<byte>^ message = ManagedByteArrayFromJavaByteArray(env, jmessage);
  try {
    ClrSystemHandlerWrapper::Call_ClrSystemTaskMessage_OnNext(handle, taskMesageBridge, message);
  }
  catch (System::Exception^ ex) {
    String^ errorMessage = "Exception in Call_ClrSystemTaskMessage_OnNext";
    ManagedLog::LOGGER->LogError(errorMessage, ex);
    taskMesageBridge -> OnError(errorMessage);
  }
}

/*
 * Class:     org_apache_reef_javabridge_NativeInterop
 * Method:    ClrSysteFailedTaskHandlerOnNext
 * Signature: (JLorg/apache/reef/javabridge/FailedTaskBridge;Lorg/apache/reef/javabridge/InteropLogger;)V
 */
JNIEXPORT void JNICALL Java_org_apache_reef_javabridge_NativeInterop_ClrSystemFailedTaskHandlerOnNext
(JNIEnv *env , jclass cls, jlong handler, jobject jfailedTask, jobject jlogger) {
  ManagedLog::LOGGER->Log("+Java_org_apache_reef_javabridge_NativeInterop_ClrSystemFailedTaskHandlerOnNext");
  FailedTaskClr2Java^ failedTaskBridge = gcnew FailedTaskClr2Java(env, jfailedTask);
  try {
    ClrSystemHandlerWrapper::Call_ClrSystemFailedTask_OnNext(handler, failedTaskBridge);
  }
  catch (System::Exception^ ex) {
    String^ errorMessage = "Exception in Call_ClrSystemTaskMessage_OnNext";
    ManagedLog::LOGGER->LogError(errorMessage, ex);
    failedTaskBridge -> OnError(errorMessage);
  }
}

/*
 * Class:     org_apache_reef_javabridge_NativeInterop
 * Method:    ClrSysteFailedTaskHandlerOnNext
 * Signature: (JLorg.apache.reef.javabridge/FailedTaskBridge;Lorg.apache.reef.javabridge/InteropLogger;)V
 */
JNIEXPORT void JNICALL Java_org_apache_reef_javabridge_NativeInterop_ClrSystemRunningTaskHandlerOnNext
(JNIEnv *env , jclass cls, jlong handler, jobject jrunningTask, jobject jlogger) {
  ManagedLog::LOGGER->Log("+Java_org_apache_reef_javabridge_NativeInterop_ClrSystemRunningTaskHandlerOnNext");
  RunningTaskClr2Java^ runningTaskBridge = gcnew RunningTaskClr2Java(env, jrunningTask);
  try {
    ClrSystemHandlerWrapper::Call_ClrSystemRunningTask_OnNext(handler, runningTaskBridge);
  }
  catch (System::Exception^ ex) {
    String^ errorMessage = "Exception in Call_ClrSystemRunningTask_OnNext";
    ManagedLog::LOGGER->LogError(errorMessage, ex);
    runningTaskBridge -> OnError(errorMessage);
  }
}

/*
 * Class:     org_apache_reef_javabridge_NativeInterop
 * Method:    ClrSystemFailedEvaluatorHandlerOnNext
 * Signature: (JLorg/apache/reef/javabridge/FailedEvaluatorBridge;Lorg/apache/reef/javabridge/InteropLogger;)V
 */
JNIEXPORT void JNICALL Java_org_apache_reef_javabridge_NativeInterop_ClrSystemFailedEvaluatorHandlerOnNext
(JNIEnv *env , jclass cls, jlong handler, jobject jfailedEvaluator, jobject jlogger) {
  ManagedLog::LOGGER->Log("+Java_org_apache_reef_javabridge_NativeInterop_ClrSystemFailedEvaluatorHandlerOnNext");
  FailedEvaluatorClr2Java^ failedEvaluatorBridge = gcnew FailedEvaluatorClr2Java(env, jfailedEvaluator);
  try {
    ClrSystemHandlerWrapper::Call_ClrSystemFailedEvaluator_OnNext(handler, failedEvaluatorBridge);
  }
  catch (System::Exception^ ex) {
    String^ errorMessage = "Exception in Call_ClrSystemFailedEvaluator_OnNext";
    ManagedLog::LOGGER->LogError(errorMessage, ex);
    failedEvaluatorBridge -> OnError(errorMessage);
  }
}

/*
 * Class:     org_apache_reef_javabridge_NativeInterop
 * Method:    ClrSystemHttpServerEventHandlerOnHttpRequest
 * Signature: (JLorg/apache/reef/javabridge/HttpServerEventBridge;Lorg/apache/reef/javabridge/InteropLogger;)V
 */
JNIEXPORT void JNICALL Java_org_apache_reef_javabridge_NativeInterop_ClrSystemHttpServerHandlerOnNext
(JNIEnv *env , jclass cls, jlong handler, jobject jhttpServerEventBridge, jobject jlogger) {
  ManagedLog::LOGGER->Log("+Java_org_apache_reef_javabridge_NativeInterop_ClrSystemHttpServerHandlerOnNext");
  HttpServerClr2Java^ httpServerClr2Java = gcnew HttpServerClr2Java(env, jhttpServerEventBridge);
  try {
    ClrSystemHandlerWrapper::Call_ClrSystemHttpServer_OnNext(handler, httpServerClr2Java);
  }
  catch (System::Exception^ ex) {
    String^ errorMessage = "Exception in Call_ClrSystemHttpServer_OnNext";
    ManagedLog::LOGGER->LogError(errorMessage, ex);
    httpServerClr2Java -> OnError(errorMessage);
  }
}

/*
 * Class:     org_apache_reef_javabridge_NativeInterop
 * Method:    ClrSystemCompletedTaskHandlerOnNext
 * Signature: (JLorg/apache/reef/javabridge/CompletedTaskBridge;Lorg/apache/reef/javabridge/InteropLogger;)V
 */
JNIEXPORT void JNICALL Java_org_apache_reef_javabridge_NativeInterop_ClrSystemCompletedTaskHandlerOnNext
(JNIEnv *env , jclass cls, jlong handler, jobject jcompletedTask, jobject jlogger) {
  ManagedLog::LOGGER->Log("+Java_org_apache_reef_javabridge_NativeInterop_ClrSystemCompletedTaskHandlerOnNext");
  CompletedTaskClr2Java^ completedTaskBridge = gcnew CompletedTaskClr2Java(env, jcompletedTask);
  try {
    ClrSystemHandlerWrapper::Call_ClrSystemCompletedTask_OnNext(handler, completedTaskBridge);
  }
  catch (System::Exception^ ex) {
    String^ errorMessage = "Exception in Call_ClrSystemCompletedTask_OnNext";
    ManagedLog::LOGGER->LogError(errorMessage, ex);
    completedTaskBridge -> OnError(errorMessage);
  }
}

/*
 * Class:     org_apache_reef_javabridge_NativeInterop
 * Method:    ClrBufferedLog
 * Signature: (ILjava/lang/String;)V
 */
JNIEXPORT void JNICALL Java_org_apache_reef_javabridge_NativeInterop_ClrBufferedLog
(JNIEnv *env, jclass cls, jint logLevel, jstring message) {
  try {
    if (!JavaClrBridge::LoggerWrapper::initialized) {
      ManagedLog::LOGGER->Log("Initializing CLRBufferedLogHandler in java bridge...");
      JavaClrBridge::LoggerWrapper::logger->Listeners->Add(gcnew System::Diagnostics::ConsoleTraceListener());
      JavaClrBridge::LoggerWrapper::initialized = true;
    }

    System::Diagnostics::TraceEventType eventType;
    switch (logLevel) {
    case 0:
      eventType = System::Diagnostics::TraceEventType::Stop;
      break;
    case 1:
      eventType = System::Diagnostics::TraceEventType::Error;
      break;
    case 2:
      eventType = System::Diagnostics::TraceEventType::Warning;
      break;
    case 3:
      eventType = System::Diagnostics::TraceEventType::Information;
      break;
    case 4:
      eventType = System::Diagnostics::TraceEventType::Verbose;
      break;
    default:
      eventType = System::Diagnostics::TraceEventType::Information;
      break;

    }

    String^ msg = ManagedStringFromJavaString(env, message);
    msg = System::String::Concat(System::DateTime::Now, msg);
    JavaClrBridge::LoggerWrapper::logger->TraceEvent(eventType, 0, msg);
  }
  catch (System::Exception^ ex) {
    ManagedLog::LOGGER->LogError("Exception in Java_javabridge_NativeInterop_ClrBufferedLog", ex);
  }
}

/*
 * Class:     org_apache_reef_javabridge_NativeInterop
 * Method:    ClrSystemSupendedTaskHandlerOnNext
 * Signature: (JLorg/apache/reef/javabridge/SuspendedTaskBridge;)V
 */
JNIEXPORT void JNICALL Java_org_apache_reef_javabridge_NativeInterop_ClrSystemSupendedTaskHandlerOnNext
(JNIEnv *env , jclass cls, jlong handler, jobject jsuspendedTask) {
  ManagedLog::LOGGER->Log("+Java_org_apache_reef_javabridge_NativeInterop_ClrSystemSupendedTaskHandlerOnNext");
  SuspendedTaskClr2Java^ suspendedTaskBridge = gcnew SuspendedTaskClr2Java(env, jsuspendedTask);
  try {
    ClrSystemHandlerWrapper::Call_ClrSystemSuspendedTask_OnNext(handler, suspendedTaskBridge);
  }
  catch (System::Exception^ ex) {
    String^ errorMessage = "Exception in Call_ClrSystemSuspendedTask_OnNext";
    ManagedLog::LOGGER->LogError(errorMessage, ex);
    suspendedTaskBridge -> OnError(errorMessage);
  }
}

/*
 * Class:     org_apache_reef_javabridge_NativeInterop
 * Method:    ClrSystemCompletdEvaluatorHandlerOnNext
 * Signature: (JLorg/apache/reef/javabridge/CompletedEvaluatorBridge;)V
 */
JNIEXPORT void JNICALL Java_org_apache_reef_javabridge_NativeInterop_ClrSystemCompletdEvaluatorHandlerOnNext
(JNIEnv *env , jclass cls, jlong handler, jobject jcompletedEvaluator) {
  ManagedLog::LOGGER->Log("+Java_org_apache_reef_javabridge_NativeInterop_ClrSystemCompletdEvaluatorHandlerOnNext");
  CompletedEvaluatorClr2Java^ completedEvaluatorBridge = gcnew CompletedEvaluatorClr2Java(env, jcompletedEvaluator);
  try {
    ClrSystemHandlerWrapper::Call_ClrSystemCompletedEvaluator_OnNext(handler, completedEvaluatorBridge);
  }
  catch (System::Exception^ ex) {
    String^ errorMessage = "Exception in Call_ClrSystemSuspendedTask_OnNext";
    ManagedLog::LOGGER->LogError(errorMessage, ex);
    completedEvaluatorBridge -> OnError(errorMessage);
  }
}

/*
 * Class:     org_apache_reef_javabridge_NativeInterop
 * Method:    ClrSystemClosedContextHandlerOnNext
 * Signature: (JLorg/apache/reef/javabridge/ClosedContextBridge;)V
 */
JNIEXPORT void JNICALL Java_org_apache_reef_javabridge_NativeInterop_ClrSystemClosedContextHandlerOnNext
(JNIEnv *env , jclass cls, jlong handler, jobject jclosedContext) {
  ManagedLog::LOGGER->Log("+Java_org_apache_reef_javabridge_NativeInterop_ClrSystemClosedContextHandlerOnNext");
  ClosedContextClr2Java^ closedContextBridge = gcnew ClosedContextClr2Java(env, jclosedContext);
  try {
    ClrSystemHandlerWrapper::Call_ClrSystemClosedContext_OnNext(handler, closedContextBridge);
  }
  catch (System::Exception^ ex) {
    String^ errorMessage = "Exception in Call_ClrSystemClosedContext_OnNext";
    ManagedLog::LOGGER->LogError(errorMessage, ex);
    closedContextBridge -> OnError(errorMessage);
  }
}

/*
 * Class:     org_apache_reef_javabridge_NativeInterop
 * Method:    ClrSystemFailedContextHandlerOnNext
 * Signature: (JLorg/apache/reef/javabridge/FailedContextBridge;)V
 */
JNIEXPORT void JNICALL Java_org_apache_reef_javabridge_NativeInterop_ClrSystemFailedContextHandlerOnNext
(JNIEnv *env , jclass cls, jlong handler, jobject jfailedContext) {
  ManagedLog::LOGGER->Log("+Java_org_apache_reef_javabridge_NativeInterop_ClrSystemFailedContextHandlerOnNext");
  FailedContextClr2Java^ failedContextBridge = gcnew FailedContextClr2Java(env, jfailedContext);
  try {
    ClrSystemHandlerWrapper::Call_ClrSystemFailedContext_OnNext(handler, failedContextBridge);
  }
  catch (System::Exception^ ex) {
    String^ errorMessage = "Exception in Call_ClrSystemFailedContext_OnNext";
    ManagedLog::LOGGER->LogError(errorMessage, ex);
    failedContextBridge -> OnError(errorMessage);
  }
}

/*
 * Class:     org_apache_reef_javabridge_NativeInterop
 * Method:    ClrSystemContextMessageHandlerOnNext
 * Signature: (JLorg/apache/reef/javabridge/ContextMessageBridge;)V
 */
JNIEXPORT void JNICALL Java_org_apache_reef_javabridge_NativeInterop_ClrSystemContextMessageHandlerOnNext
(JNIEnv *env , jclass cls, jlong handler, jobject jcontextMessage) {
  ManagedLog::LOGGER->Log("+Java_org_apache_reef_javabridge_NativeInterop_ClrSystemContextMessageHandlerOnNext");
  ContextMessageClr2Java^ contextMessageBridge = gcnew ContextMessageClr2Java(env, jcontextMessage);
  try {
    ClrSystemHandlerWrapper::Call_ClrSystemContextMessage_OnNext(handler, contextMessageBridge);
  }
  catch (System::Exception^ ex) {
    String^ errorMessage = "Exception in Call_ClrSystemContextMessage_OnNext";
    ManagedLog::LOGGER->LogError(errorMessage, ex);
    contextMessageBridge -> OnError(errorMessage);
  }
}

/*
 * Class:     org_apache_reef_javabridge_NativeInterop
 * Method:    ClrSystemDriverRestartHandlerOnNext
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_apache_reef_javabridge_NativeInterop_ClrSystemDriverRestartHandlerOnNext
(JNIEnv *env , jclass cls, jlong handler) {
  ManagedLog::LOGGER->Log("+Java_org_apache_reef_javabridge_NativeInterop_ClrSystemDriverRestartHandlerOnNext");
  try {
    ClrSystemHandlerWrapper::Call_ClrSystemDriverRestart_OnNext(handler);
  }
  catch (System::Exception^ ex) {
    String^ errorMessage = "Exception in Call_ClrSystemContextMessage_OnNext";
    ManagedLog::LOGGER->LogError(errorMessage, ex);
    // we do not call back to Java for exception in .NET restart handler
  }
}

/*
 * Class:     org_apache_reef_javabridge_NativeInterop
 * Method:    ClrSystemDriverRestartActiveContextHandlerOnNext
 * Signature: (JLorg/apache/reef/javabridge/ActiveContextBridge;)V
 */
JNIEXPORT void JNICALL Java_org_apache_reef_javabridge_NativeInterop_ClrSystemDriverRestartActiveContextHandlerOnNext
(JNIEnv *env, jclass cls, jlong handle, jobject jactiveContextBridge) {
  ManagedLog::LOGGER->Log("+Java_org_apache_reef_javabridge_NativeInterop_ClrSystemDriverRestartActiveContextHandlerOnNext");
  ActiveContextClr2Java^ activeContextBrdige = gcnew ActiveContextClr2Java(env, jactiveContextBridge);
  try {
    ClrSystemHandlerWrapper::Call_ClrSystemDriverRestartActiveContextHandler_OnNext(handle, activeContextBrdige);
  }
  catch (System::Exception^ ex) {
    String^ errorMessage = "Exception in Call_ClrSystemDriverRestartActiveContextHandler_OnNext";
    ManagedLog::LOGGER -> LogError(errorMessage, ex);
    activeContextBrdige -> OnError(errorMessage);
  }
}

/*
 * Class:     org_apache_reef_javabridge_NativeInterop
 * Method:    ClrSystemDriverRestartRunningTaskHandlerOnNext
 * Signature: (JLorg/apache/reef/javabridge/RunningTaskBridge;)V
 */
JNIEXPORT void JNICALL Java_org_apache_reef_javabridge_NativeInterop_ClrSystemDriverRestartRunningTaskHandlerOnNext
(JNIEnv *env , jclass cls, jlong handler, jobject jrunningTask) {
  ManagedLog::LOGGER->Log("+Java_org_apache_reef_javabridge_NativeInterop_ClrSystemDriverRestartRunningTaskHandlerOnNext");
  RunningTaskClr2Java^ runningTaskBridge = gcnew RunningTaskClr2Java(env, jrunningTask);
  try {
    ClrSystemHandlerWrapper::Call_ClrSystemDriverRestartRunningTask_OnNext(handler, runningTaskBridge);
  }
  catch (System::Exception^ ex) {
    String^ errorMessage = "Exception in Call_ClrSystemDriverRestartRunningTask_OnNext";
    ManagedLog::LOGGER->LogError(errorMessage, ex);
    runningTaskBridge -> OnError(errorMessage);
  }
}