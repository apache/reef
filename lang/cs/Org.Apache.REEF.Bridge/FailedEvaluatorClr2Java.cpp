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

#include "Clr2JavaImpl.h"

using namespace JavaClrBridge;

namespace Org {
  namespace Apache {
	  namespace REEF {
		  namespace Driver {
			  namespace Bridge {
				  namespace Clr2java {
					  ref class ManagedLog {
					  internal:
						  static BridgeLogger^ LOGGER = BridgeLogger::GetLogger("<C++>");
					  };

					  FailedEvaluatorClr2Java::FailedEvaluatorClr2Java(JNIEnv *env, jobject jobjectFailedEvaluator) {
						  ManagedLog::LOGGER->LogStart("FailedEvaluatorClr2Java::FailedEvaluatorClr2Java");
						  pin_ptr<JavaVM*> pJavaVm = &_jvm;
						  if (env->GetJavaVM(pJavaVm) != 0) {
							  ManagedLog::LOGGER->LogError("Failed to get JavaVM", nullptr);
						  }
						  _jobjectFailedEvaluator = reinterpret_cast<jobject>(env->NewGlobalRef(jobjectFailedEvaluator));

						  jclass jclassFailedEvaluator = env->GetObjectClass(_jobjectFailedEvaluator);
						  _jstringId = CommonUtilities::GetJObjectId(env, _jobjectFailedEvaluator, jclassFailedEvaluator);
						  ManagedLog::LOGGER->LogStop("FailedEvaluatorClr2Java::FailedEvaluatorClr2Java");
					  }

					  FailedEvaluatorClr2Java::~FailedEvaluatorClr2Java() {
						  this->!FailedEvaluatorClr2Java();
					  }

					  FailedEvaluatorClr2Java::!FailedEvaluatorClr2Java() {
						  JNIEnv *env = RetrieveEnv(_jvm);
						  if (_jobjectFailedEvaluator != NULL) {
							  env->DeleteGlobalRef(_jobjectFailedEvaluator);
						  }

						  if (_jstringId != NULL) {
							  env->DeleteGlobalRef(_jstringId);
						  }
					  }

					  IEvaluatorRequestorClr2Java^ FailedEvaluatorClr2Java::GetEvaluatorRequestor() {
						  ManagedLog::LOGGER->LogStart("FailedEvaluatorClr2Java::GetEvaluatorRequestor");
						  JNIEnv *env = RetrieveEnv(_jvm);

						  jclass jclassFailedEvaluator = env->GetObjectClass(_jobjectFailedEvaluator);
						  jmethodID jmidGetEvaluatorRequestor = env->GetMethodID(jclassFailedEvaluator, "getEvaluatorRequestorBridge", "()Lorg/apache/reef/javabridge/EvaluatorRequestorBridge;");
						  jobject jobjectEvaluatorRequestor = env->CallObjectMethod(_jobjectFailedEvaluator, jmidGetEvaluatorRequestor);
						  ManagedLog::LOGGER->LogStop("FailedEvaluatorClr2Java::GetEvaluatorRequestor");
						  return gcnew EvaluatorRequestorClr2Java(env, jobjectEvaluatorRequestor);
					  }

					  String^ FailedEvaluatorClr2Java::GetId() {
						  ManagedLog::LOGGER->Log("FailedEvaluatorClr2Java::GetId");

						  JNIEnv *env = RetrieveEnv(_jvm);
						  return ManagedStringFromJavaString(env, _jstringId);
					  }

                      array<IFailedContextClr2Java^>^ FailedEvaluatorClr2Java::GetFailedContextsClr2Java() {
                          JNIEnv *env = RetrieveEnv(_jvm);
                          jclass jclassFailedEvaluator = env->GetObjectClass(_jobjectFailedEvaluator);
                          jmethodID jmidGetFailedContexts = 
                              env->GetMethodID(jclassFailedEvaluator, "getFailedContexts", "()[Lorg/apache/reef/javabridge/FailedContextBridge;");
                          jobjectArray failedContexts = reinterpret_cast<jobjectArray>(env->CallObjectMethod(_jobjectFailedEvaluator, jmidGetFailedContexts));
                          
                          const int arrLen = env->GetArrayLength(failedContexts);
                          
                          array<IFailedContextClr2Java^>^ failedContextsArray = gcnew array<IFailedContextClr2Java^>(arrLen);
                          for (int i = 0; i < arrLen; i++) {
                              jobject failedContext = env->GetObjectArrayElement(failedContexts, i);
                              failedContextsArray[i] = gcnew FailedContextClr2Java(env, failedContext);
                          }

                          return failedContextsArray;
                      }

                      IFailedTaskClr2Java^ FailedEvaluatorClr2Java::GetFailedTaskClr2Java() {
                          ManagedLog::LOGGER->Log("FailedEvaluatorClr2Java::GetFailedTaskClr2Java");
                          JNIEnv *env = RetrieveEnv(_jvm);
                          jclass jclassFailedEvaluator = env->GetObjectClass(_jobjectFailedEvaluator);

                          jmethodID jmidGetFailedTask =
                              env->GetMethodID(jclassFailedEvaluator, "getFailedTask", "()Lorg/apache/reef/javabridge/FailedTaskBridge;");

                          jobject failedTask = env->CallObjectMethod(_jobjectFailedEvaluator, jmidGetFailedTask);
                          if (failedTask == NULL) {
                              return nullptr;
                          }

                          return gcnew FailedTaskClr2Java(env, failedTask);
                      }

					  void FailedEvaluatorClr2Java::OnError(String^ message) {
						  ManagedLog::LOGGER->Log("FailedEvaluatorClr2Java::OnError");
						  JNIEnv *env = RetrieveEnv(_jvm);
						  HandleClr2JavaError(env, message, _jobjectFailedEvaluator);
					  }

                      array<byte>^ FailedEvaluatorClr2Java::GetErrorBytes() {
                          JNIEnv *env = RetrieveEnv(_jvm);
                          jclass jclassFailedEvaluator = env->GetObjectClass(_jobjectFailedEvaluator);
                          jmethodID jmidGetError = env->GetMethodID(jclassFailedEvaluator, "getErrorBytes", "()[B");
                          jobject methodCallReturn = env->CallObjectMethod(_jobjectFailedEvaluator, jmidGetError);
                          if (methodCallReturn == NULL) {
                              return nullptr;
                          }

                          jbyteArray error = reinterpret_cast<jbyteArray>(methodCallReturn);
                          return ManagedByteArrayFromJavaByteArray(env, error);
                      }

                      String^ FailedEvaluatorClr2Java::GetJavaCause() {
                          JNIEnv *env = RetrieveEnv(_jvm);
                          jclass jclassFailedEvaluator = env->GetObjectClass(_jobjectFailedEvaluator);
                          jmethodID jmidGetCause = env->GetMethodID(jclassFailedEvaluator, "getCause", "()Ljava/lang/String;");
                          jobject methodCallReturn = env->CallObjectMethod(_jobjectFailedEvaluator, jmidGetCause);
                          if (methodCallReturn == NULL) {
                              return nullptr;
                          }

                          jstring cause = reinterpret_cast<jstring>(methodCallReturn);
                          return ManagedStringFromJavaString(env, cause);
                      }

                      String^ FailedEvaluatorClr2Java::GetJavaStackTrace() {
                          JNIEnv *env = RetrieveEnv(_jvm);
                          jclass jclassFailedEvaluator = env->GetObjectClass(_jobjectFailedEvaluator);
                          jmethodID jmidGetStackTrace = env->GetMethodID(jclassFailedEvaluator, "getStackTrace", "()Ljava/lang/String;");
                          jobject methodCallReturn = env->CallObjectMethod(_jobjectFailedEvaluator, jmidGetStackTrace);
                          if (methodCallReturn == NULL) {
                              return nullptr;
                          }

                          jstring stackTrace = reinterpret_cast<jstring>(methodCallReturn);
                          return ManagedStringFromJavaString(env, stackTrace);
                      }
				  }
			  }
		  }
	  }
  }
}