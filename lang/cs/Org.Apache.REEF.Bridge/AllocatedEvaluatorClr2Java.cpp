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

            AllocatedEvaluatorClr2Java::AllocatedEvaluatorClr2Java(JNIEnv *env, jobject jallocatedEvaluator, jstring nameServerInfo, jstring evaluatorId) {

              ManagedLog::LOGGER->LogStart("AllocatedEvaluatorClr2Java::AllocatedEvaluatorClr2Java");

              pin_ptr<JavaVM*> pJavaVm = &_jvm;
              if (env->GetJavaVM(pJavaVm) != 0) {
                ManagedLog::LOGGER->LogError("Failed to get JavaVM", nullptr);
              }
              _jobjectAllocatedEvaluator = reinterpret_cast<jobject>(env->NewGlobalRef(jallocatedEvaluator));

			  _evaluatorId = ManagedStringFromJavaString(env, evaluatorId);
			  _nameServerInfo = ManagedStringFromJavaString(env, nameServerInfo);

              ManagedLog::LOGGER->LogStop("AllocatedEvaluatorClr2Java::AllocatedEvaluatorClr2Java");
            }

            AllocatedEvaluatorClr2Java::~AllocatedEvaluatorClr2Java() {
              this->!AllocatedEvaluatorClr2Java();
            }

            AllocatedEvaluatorClr2Java::!AllocatedEvaluatorClr2Java() {
              JNIEnv *env = RetrieveEnv(_jvm);
              if (_jobjectAllocatedEvaluator != NULL) {
                env->DeleteGlobalRef(_jobjectAllocatedEvaluator);
              }
            }

            void AllocatedEvaluatorClr2Java::SubmitContext(String^ evaluatorConfigStr, String^ contextConfigStr) {
              ManagedLog::LOGGER->LogStart("AllocatedEvaluatorClr2Java::SubmitContext");
              JNIEnv *env = RetrieveEnv(_jvm);
              jclass jclassAllocatedEvaluator = env->GetObjectClass(_jobjectAllocatedEvaluator);
              jmethodID jmidSubmitContext = env->GetMethodID(jclassAllocatedEvaluator, "submitContextString", "(Ljava/lang/String;Ljava/lang/String;)V");

              if (jmidSubmitContext == NULL) {
                ManagedLog::LOGGER->Log("jmidSubmitContext is NULL");
                return;
              }
              env->CallObjectMethod(
                _jobjectAllocatedEvaluator,
                jmidSubmitContext,
                JavaStringFromManagedString(env, evaluatorConfigStr),
                JavaStringFromManagedString(env, contextConfigStr));
              ManagedLog::LOGGER->LogStop("AllocatedEvaluatorClr2Java::SubmitContext");
            }

            void AllocatedEvaluatorClr2Java::SubmitContextAndTask(String^ evaluatorConfigStr, String^ contextConfigStr, String^ taskConfigStr) {
              ManagedLog::LOGGER->LogStart("AllocatedEvaluatorClr2Java::SubmitContextAndTask" + taskConfigStr);
              JNIEnv *env = RetrieveEnv(_jvm);
              jclass jclassAllocatedEvaluator = env->GetObjectClass(_jobjectAllocatedEvaluator);
              jmethodID jmidSubmitContextAndTask = env->GetMethodID(jclassAllocatedEvaluator, "submitContextAndTaskString", "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)V");

              if (jmidSubmitContextAndTask == NULL) {
                ManagedLog::LOGGER->Log("jmidSubmitContextAndTask is NULL");
                return;
              }
              env->CallObjectMethod(
                _jobjectAllocatedEvaluator,
                jmidSubmitContextAndTask,
                JavaStringFromManagedString(env, evaluatorConfigStr),
                JavaStringFromManagedString(env, contextConfigStr),
                JavaStringFromManagedString(env, taskConfigStr));
              ManagedLog::LOGGER->LogStop("AllocatedEvaluatorClr2Java::SubmitContextAndTask" + taskConfigStr);
            }

            void AllocatedEvaluatorClr2Java::SubmitContextAndService(String^ evaluatorConfigStr, String^ contextConfigStr, String^ serviceConfigStr) {
              ManagedLog::LOGGER->LogStart("AllocatedEvaluatorClr2Java::SubmitContextAndService");
              JNIEnv *env = RetrieveEnv(_jvm);
              jclass jclassAllocatedEvaluator = env->GetObjectClass(_jobjectAllocatedEvaluator);
              jmethodID jmidSubmitContextAndService = env->GetMethodID(jclassAllocatedEvaluator, "submitContextAndServiceString", "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)V");

              if (jmidSubmitContextAndService == NULL) {
                ManagedLog::LOGGER->Log("jmidSubmitContextAndService is NULL");
                return;
              }
              env->CallObjectMethod(
                _jobjectAllocatedEvaluator,
                jmidSubmitContextAndService,
                JavaStringFromManagedString(env, evaluatorConfigStr),
                JavaStringFromManagedString(env, contextConfigStr),
                JavaStringFromManagedString(env, serviceConfigStr));
              ManagedLog::LOGGER->LogStop("AllocatedEvaluatorClr2Java::SubmitContextAndService");
            }

            void AllocatedEvaluatorClr2Java::SubmitContextAndServiceAndTask(String^ evaluatorConfigStr, String^ contextConfigStr, String^ serviceConfigStr, String^ taskConfigStr) {
              ManagedLog::LOGGER->LogStart("AllocatedEvaluatorClr2Java::SubmitContextAndServiceAndTask");
              JNIEnv *env = RetrieveEnv(_jvm);
              jclass jclassAllocatedEvaluator = env->GetObjectClass(_jobjectAllocatedEvaluator);
              jmethodID jmidSubmitContextAndServiceAndTask = env->GetMethodID(jclassAllocatedEvaluator, "submitContextAndServiceAndTaskString", "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)V");

              if (jmidSubmitContextAndServiceAndTask == NULL) {
                ManagedLog::LOGGER->Log("jmidSubmitContextAndServiceAndTask is NULL");
                return;
              }
              env->CallObjectMethod(
                _jobjectAllocatedEvaluator,
                jmidSubmitContextAndServiceAndTask,
                JavaStringFromManagedString(env, evaluatorConfigStr),
                JavaStringFromManagedString(env, contextConfigStr),
                JavaStringFromManagedString(env, serviceConfigStr),
                JavaStringFromManagedString(env, taskConfigStr));
              ManagedLog::LOGGER->LogStop("AllocatedEvaluatorClr2Java::SubmitContextAndServiceAndTask");
            }

            void AllocatedEvaluatorClr2Java::OnError(String^ message) {
              JNIEnv *env = RetrieveEnv(_jvm);
              HandleClr2JavaError(env, message, _jobjectAllocatedEvaluator);
            }

            void AllocatedEvaluatorClr2Java::Close() {
              ManagedLog::LOGGER->LogStart("AllocatedEvaluatorClr2Java::Close");
              JNIEnv *env = RetrieveEnv(_jvm);
              jclass jclassAllocatedEvaluator = env->GetObjectClass(_jobjectAllocatedEvaluator);
              jmethodID jmidClose = env->GetMethodID(jclassAllocatedEvaluator, "close", "()V");

              if (jmidClose == NULL) {
                ManagedLog::LOGGER->Log("jmidClose is NULL");
                return;
              }
              env->CallObjectMethod(
                _jobjectAllocatedEvaluator,
                jmidClose);
              ManagedLog::LOGGER->LogStop("AllocatedEvaluatorClr2Java::Close");
            }

            String^ AllocatedEvaluatorClr2Java::GetId() {
				return _evaluatorId;
            }

            String^ AllocatedEvaluatorClr2Java::GetNameServerInfo() {
				return _nameServerInfo;
            }

            IEvaluatorDescriptor^ AllocatedEvaluatorClr2Java::GetEvaluatorDescriptor() {
              ManagedLog::LOGGER->LogStart("AllocatedEvaluatorClr2Java::GetEvaluatorDescriptor");
              return CommonUtilities::RetrieveEvaluatorDescriptor(_jobjectAllocatedEvaluator, _jvm);
            }
          }
        }
      }
    }
  }
}