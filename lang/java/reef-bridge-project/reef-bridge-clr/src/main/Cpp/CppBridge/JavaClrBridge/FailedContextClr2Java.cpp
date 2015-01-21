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

namespace Microsoft {
  namespace Reef {
    namespace Driver {
      namespace Bridge {
        ref class ManagedLog {
          internal:
            static BridgeLogger^ LOGGER = BridgeLogger::GetLogger("<C++>");
        };
        FailedContextClr2Java::FailedContextClr2Java(JNIEnv *env, jobject jobjectFailedContext) {
          ManagedLog::LOGGER->LogStart("FailedContextClr2Java::FailedContextClr2Java");

          pin_ptr<JavaVM*> pJavaVm = &_jvm;
          if (env->GetJavaVM(pJavaVm) != 0) {
            ManagedLog::LOGGER->LogError("Failed to get JavaVM", nullptr);
          }
          _jobjectFailedContext = reinterpret_cast<jobject>(env->NewGlobalRef(jobjectFailedContext));
          jclass jclassFailedContext = env->GetObjectClass (_jobjectFailedContext);

          jfieldID jidContextId = env->GetFieldID(jclassFailedContext, "contextId", "Ljava/lang/String;");
          jfieldID jidEvaluatorId = env->GetFieldID(jclassFailedContext, "evaluatorId", "Ljava/lang/String;");
          jfieldID jidParentId = env->GetFieldID(jclassFailedContext, "parentContextId", "Ljava/lang/String;");

          _jstringContextId = reinterpret_cast<jstring>(env->NewGlobalRef(env->GetObjectField(_jobjectFailedContext, jidContextId)));
          _jstringEvaluatorId = reinterpret_cast<jstring>(env->NewGlobalRef(env->GetObjectField(_jobjectFailedContext, jidEvaluatorId)));
          _jstringParentContextId = reinterpret_cast<jstring>(env->NewGlobalRef(env->GetObjectField(_jobjectFailedContext, jidParentId)));

          ManagedLog::LOGGER->LogStop("FailedContextClr2Java::FailedContextClr2Java");
        }

        IActiveContextClr2Java^ FailedContextClr2Java::GetParentContext() {
          ManagedLog::LOGGER->LogStart("FailedContextClr2Java::GetParentContext");

          JNIEnv *env = RetrieveEnv(_jvm);

          jclass jclassFailedContext = env->GetObjectClass(_jobjectFailedContext);
          jfieldID jidParentContext = env->GetFieldID(jclassFailedContext, "parentContext", "Lorg/apache/reef/javabridge/ActiveContextBridge;");
          jobject jobjectParentContext = env->GetObjectField(_jobjectFailedContext, jidParentContext);
          ManagedLog::LOGGER->LogStop("FailedContextClr2Java::GetParentContext");

          return gcnew ActiveContextClr2Java(env, jobjectParentContext);
        }

        String^ FailedContextClr2Java::GetId() {
          ManagedLog::LOGGER->Log("FailedContextClr2Java::GetId");
          JNIEnv *env = RetrieveEnv(_jvm);
          return ManagedStringFromJavaString(env, _jstringContextId);
        }

        String^ FailedContextClr2Java::GetEvaluatorId() {
          ManagedLog::LOGGER->Log("FailedContextClr2Java::GetEvaluatorId");
          JNIEnv *env = RetrieveEnv(_jvm);
          return ManagedStringFromJavaString(env, _jstringEvaluatorId);
        }

        String^ FailedContextClr2Java::GetParentId() {
          ManagedLog::LOGGER->Log("FailedContextClr2Java::GetParentId");
          JNIEnv *env = RetrieveEnv(_jvm);
          return ManagedStringFromJavaString(env, _jstringParentContextId);
        }

        IEvaluatorDescriptor^ FailedContextClr2Java::GetEvaluatorDescriptor() {
          ManagedLog::LOGGER->LogStart("FailedContextClr2Java::GetEvaluatorDescriptor");
          return CommonUtilities::RetrieveEvaluatorDescriptor(_jobjectFailedContext, _jvm);
        }

        void FailedContextClr2Java::OnError(String^ message) {
          ManagedLog::LOGGER->Log("FailedContextClr2Java::OnError");
          JNIEnv *env = RetrieveEnv(_jvm);
          HandleClr2JavaError(env, message, _jobjectFailedContext);
        }
      }
    }
  }
}