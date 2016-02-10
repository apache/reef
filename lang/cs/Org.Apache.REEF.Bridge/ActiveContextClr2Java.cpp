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

namespace Org {
	namespace Apache {
		namespace REEF {
			namespace Driver {
				namespace Bridge {
					namespace Clr2java {
						private ref class ManagedLog {
						internal:
							static BridgeLogger^ LOGGER = BridgeLogger::GetLogger("<C++>");
						};

						ActiveContextClr2Java::ActiveContextClr2Java(JNIEnv *env, jobject jobjectActiveContext) {
							ManagedLog::LOGGER->LogStart("ActiveContextClr2Java::ActiveContextClr2Java");
							pin_ptr<JavaVM*> pJavaVm = &_jvm;
							if (env->GetJavaVM(pJavaVm) != 0) {
								ManagedLog::LOGGER->LogError("Failed to get JavaVM", nullptr);
							}

							_jobjectActiveContext = reinterpret_cast<jobject>(env->NewGlobalRef(jobjectActiveContext));

							jclass jclassActiveContext = env->GetObjectClass(_jobjectActiveContext);
							jmethodID jmidGetContextId = env->GetMethodID(jclassActiveContext, "getContextId", "()Ljava/lang/String;");
							_jstringId = CommonUtilities::CallGetMethodNewGlobalRef<jstring>(env, _jobjectActiveContext, jmidGetContextId);
							jmethodID jmidGetParentContextId = env->GetMethodID(jclassActiveContext, "getParentContextId", "()Ljava/lang/String;");
							_jstringParentId = CommonUtilities::CallGetMethodNewGlobalRef<jstring>(env, _jobjectActiveContext, jmidGetParentContextId);
							_jstringEvaluatorId = CommonUtilities::GetJObjectEvaluatorId(env, _jobjectActiveContext, jclassActiveContext);

							ManagedLog::LOGGER->LogStop("ActiveContextClr2Java::ActiveContextClr2Java");
						}

						ActiveContextClr2Java::~ActiveContextClr2Java() {
							this->!ActiveContextClr2Java();
						}

						ActiveContextClr2Java::!ActiveContextClr2Java() {
							JNIEnv *env = RetrieveEnv(_jvm);
							if (_jobjectActiveContext != NULL) {
								env->DeleteGlobalRef(_jobjectActiveContext); 
							}

							if (_jstringId != NULL) {
								env->DeleteGlobalRef(_jstringId);
							}

							if (_jstringEvaluatorId != NULL) {
								env->DeleteGlobalRef(_jstringEvaluatorId);
							}
						}

						void ActiveContextClr2Java::SubmitTask(String^ taskConfigStr) {
							ManagedLog::LOGGER->LogStart("ActiveContextClr2Java::SubmitTask");
							JNIEnv *env = RetrieveEnv(_jvm);
							jclass jclassActiveContext = env->GetObjectClass(_jobjectActiveContext);
							jmethodID jmidSubmitTask = env->GetMethodID(jclassActiveContext, "submitTaskString", "(Ljava/lang/String;)V");

							if (jmidSubmitTask == NULL) {
								ManagedLog::LOGGER->Log("jmidSubmitTask is NULL");
								return;
							}
							env->CallObjectMethod(
								_jobjectActiveContext,
								jmidSubmitTask,
								JavaStringFromManagedString(env, taskConfigStr));
							ManagedLog::LOGGER->LogStop("ActiveContextClr2Java::SubmitTask");
						}

						void ActiveContextClr2Java::SubmitContext(String^ contextConfigStr) {
							ManagedLog::LOGGER->LogStart("ActiveContextClr2Java::SubmitContext");
							JNIEnv *env = RetrieveEnv(_jvm);
							jclass jclassActiveContext = env->GetObjectClass(_jobjectActiveContext);
							jmethodID jmidSubmitContext = env->GetMethodID(jclassActiveContext, "submitContextString", "(Ljava/lang/String;)V");

							if (jmidSubmitContext == NULL) {
								ManagedLog::LOGGER->Log("jmidSubmitContext is NULL");
								return;
							}
							env->CallObjectMethod(
								_jobjectActiveContext,
								jmidSubmitContext,
								JavaStringFromManagedString(env, contextConfigStr));
							ManagedLog::LOGGER->LogStop("ActiveContextClr2Java::SubmitContext");
						}

						void ActiveContextClr2Java::OnError(String^ message) {
							JNIEnv *env = RetrieveEnv(_jvm);
							HandleClr2JavaError(env, message, _jobjectActiveContext);
						}

						void ActiveContextClr2Java::Close() {
							ManagedLog::LOGGER->LogStart("ActiveContextClr2Java::Close");
							JNIEnv *env = RetrieveEnv(_jvm);
							jclass jclassActiveContext = env->GetObjectClass(_jobjectActiveContext);
							jmethodID jmidClose = env->GetMethodID(jclassActiveContext, "close", "()V");

							if (jmidClose == NULL) {
								ManagedLog::LOGGER->Log("jmidClose is NULL");
								return;
							}
							env->CallObjectMethod(
								_jobjectActiveContext,
								jmidClose);
							ManagedLog::LOGGER->LogStop("ActiveContextClr2Java::Close");
						}

						String^ ActiveContextClr2Java::GetParentId() {
							JNIEnv *env = RetrieveEnv(_jvm);
							return ManagedStringFromJavaString(env, _jstringParentId);
						}

						String^ ActiveContextClr2Java::GetId() {
							JNIEnv *env = RetrieveEnv(_jvm);
							return ManagedStringFromJavaString(env, _jstringId);
						}

						String^ ActiveContextClr2Java::GetEvaluatorId() {
							JNIEnv *env = RetrieveEnv(_jvm);
							return ManagedStringFromJavaString(env, _jstringEvaluatorId);
						}

						IEvaluatorDescriptor^ ActiveContextClr2Java::GetEvaluatorDescriptor() {
							ManagedLog::LOGGER->LogStart("ActiveContextClr2Java::GetEvaluatorDescriptor");
							return CommonUtilities::RetrieveEvaluatorDescriptor(_jobjectActiveContext, _jvm);
						}

						void ActiveContextClr2Java::SendMessage(array<byte>^ message) {
							ManagedLog::LOGGER->LogStart("ActiveContextClr2Java::SendMessage");
							JNIEnv *env = RetrieveEnv(_jvm);
							jclass jclassActiveContext = env->GetObjectClass(_jobjectActiveContext);
							jmethodID jmidSendMessage = env->GetMethodID(jclassActiveContext, "sendMessage", "([B)V");

							if (jmidSendMessage == NULL) {
								ManagedLog::LOGGER->Log("jmidSendMessage is NULL");
								return;
							}

							env->CallObjectMethod(
								_jobjectActiveContext,
								jmidSendMessage,
								JavaByteArrayFromManagedByteArray(env, message));

							ManagedLog::LOGGER->LogStop("ActiveContextClr2Java::SendMessage");
						}
					}
				}
			}
		}
	}
}