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
#include "Clr2JavaImpl.h"

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

					  HttpServerClr2Java::HttpServerClr2Java(JNIEnv *env, jobject jhttpServerEventBridge) {
						  ManagedLog::LOGGER->LogStart("HttpServerClr2Java::HttpServerClr2Java");
						  pin_ptr<JavaVM*> pJavaVm = &_jvm;
						  if (env->GetJavaVM(pJavaVm) != 0) {
							  ManagedLog::LOGGER->LogError("Failed to get JavaVM", nullptr);
						  }
						  _jhttpServerEventBridge = reinterpret_cast<jobject>(env->NewGlobalRef(jhttpServerEventBridge));
						  ManagedLog::LOGGER->LogStop("HttpServerClr2Java::HttpServerClr2Java");
					  }

					  HttpServerClr2Java::~HttpServerClr2Java() {
						  this->!HttpServerClr2Java();
					  }

					  HttpServerClr2Java::!HttpServerClr2Java() {
						  if (_jhttpServerEventBridge != NULL) {
							  JNIEnv *env = RetrieveEnv(_jvm);
							  env->DeleteGlobalRef(_jhttpServerEventBridge); 
						  }
					  }

					  String^ HttpServerClr2Java::GetQueryString() {
						  ManagedLog::LOGGER->LogStart("HttpServerClr2Java::GetQueryString");
						  JNIEnv *env = RetrieveEnv(_jvm);
						  jclass jclasshttpServerEventBridge = env->GetObjectClass(_jhttpServerEventBridge);
						  jmethodID jmidgetQueryString = env->GetMethodID(jclasshttpServerEventBridge, "getQueryString", "()Ljava/lang/String;");
						  if (jmidgetQueryString == NULL) {
							  fprintf(stdout, " jmidgetQueryString is NULL\n");
							  fflush(stdout);
							  return nullptr;
						  }
						  jstring jQueryString = (jstring)env->CallObjectMethod(
							  _jhttpServerEventBridge,
							  jmidgetQueryString);

						  ManagedLog::LOGGER->LogStop("HttpServerClr2Java::GetQueryString");
						  return ManagedStringFromJavaString(env, jQueryString);
					  }

					  array<byte>^ HttpServerClr2Java::GetQueryRequestData() {
						  ManagedLog::LOGGER->LogStart("HttpServerClr2Java::GetQueryRequestData");
						  JNIEnv *env = RetrieveEnv(_jvm);
						  jclass jclasshttpServerEventBridge = env->GetObjectClass(_jhttpServerEventBridge);
						  jmethodID jmidgetQueryBytes = env->GetMethodID(jclasshttpServerEventBridge, "getQueryRequestData", "()[B");

						  if (jmidgetQueryBytes == NULL) {
							  ManagedLog::LOGGER->Log("jmidgetQueryBytes is NULL");
							  return nullptr;
						  }
						  jbyteArray jQueryBytes = (jbyteArray)env->CallObjectMethod(
							  _jhttpServerEventBridge,
							  jmidgetQueryBytes);

						  ManagedLog::LOGGER->LogStop("HttpServerClr2Java::GetQueryRequestData");
						  return ManagedByteArrayFromJavaByteArray(env, jQueryBytes);
					  }

					  void HttpServerClr2Java::SetQueryResult(String^ queryResult) {
						  ManagedLog::LOGGER->LogStart("HttpServerClr2Java::SetQueryResult");
						  JNIEnv *env = RetrieveEnv(_jvm);
						  jclass jclasshttpServerEventBridge = env->GetObjectClass(_jhttpServerEventBridge);
						  jmethodID jmidsetQueryResult = env->GetMethodID(jclasshttpServerEventBridge, "setQueryResult", "(Ljava/lang/String;)V");

						  if (jmidsetQueryResult == NULL) {
							  ManagedLog::LOGGER->Log("jmidsetQueryResult is NULL");
							  return;
						  }
						  env->CallObjectMethod(
							  _jhttpServerEventBridge,
							  jmidsetQueryResult,
							  JavaStringFromManagedString(env, queryResult));
						  ManagedLog::LOGGER->LogStop("HttpServerClr2Java::SetQueryResult");
					  }

					  void HttpServerClr2Java::SetQueryResponseData(array<byte>^ queryResponseData) {
						  ManagedLog::LOGGER->LogStart("HttpServerClr2Java::SetQueryResponseData");
						  JNIEnv *env = RetrieveEnv(_jvm);
						  jclass jclasshttpServerEventBridge = env->GetObjectClass(_jhttpServerEventBridge);
						  jmethodID jmidsetQueryResult = env->GetMethodID(jclasshttpServerEventBridge, "setQueryResponseData", "([B)V");

						  if (jmidsetQueryResult == NULL) {
							  ManagedLog::LOGGER->Log("jmidsetQueryResult is NULL");
							  return;
						  }
						  env->CallObjectMethod(
							  _jhttpServerEventBridge,
							  jmidsetQueryResult,
							  JavaByteArrayFromManagedByteArray(env, queryResponseData));
						  ManagedLog::LOGGER->LogStop("HttpServerClr2Java::SetQueryResponseData");
					  }

					  void HttpServerClr2Java::SetUriSpecification(String^ uriSpecification) {
						  ManagedLog::LOGGER->LogStart("HttpServerClr2Java::SetUriSpecification");
						  JNIEnv *env = RetrieveEnv(_jvm);
						  jclass jclasshttpServerEventBridge = env->GetObjectClass(_jhttpServerEventBridge);
						  jmethodID jmidsetUriSpecification = env->GetMethodID(jclasshttpServerEventBridge, "setUriSpecification", "(Ljava/lang/String;)V");

						  if (jmidsetUriSpecification == NULL) {
							  ManagedLog::LOGGER->Log("jmidsetUriSpecification is NULL");
							  return;
						  }
						  env->CallObjectMethod(
							  _jhttpServerEventBridge,
							  jmidsetUriSpecification,
							  JavaStringFromManagedString(env, uriSpecification));
						  ManagedLog::LOGGER->LogStop("HttpServerClr2Java::SetUriSpecification");
					  }

					  void HttpServerClr2Java::OnError(String^ message) {
						  ManagedLog::LOGGER->Log("HttpServerClr2Java::OnError");
						  JNIEnv *env = RetrieveEnv(_jvm);
						  HandleClr2JavaError(env, message, _jhttpServerEventBridge);
					  }
				  }
			  }
		  }
	  }
  }
}