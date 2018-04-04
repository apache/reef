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

#include "InteropUtil.h"
using namespace std;

// Function signatuire for creating java vm
typedef jint(JNICALL *JNI_CreateJavaVM_FN)(JavaVM **pvm, void **penv, void *args);

// The environment variable that points to java directory.
LPCTSTR JAVA_HOME = L"JAVA_HOME";

// Where should we try to load jvm dll from?
// Try these paths relative to $(JAVA_HOME).
LPCTSTR const JVM_DLL_PATHS[] = {
    L"\\jre\\bin\\server\\jvm.dll",
    L"\\jre\\bin\\client\\jvm.dll",
    L"\\bin\\server\\jvm.dll",
    L"\\bin\\client\\jvm.dll" };
int JVM_DLL_PATHS_SIZE = sizeof(JVM_DLL_PATHS) / sizeof(LPCTSTR);

// Name of the function that creates a java VM
const char* JNI_CreateJavaVM_Func_Name = "JNI_CreateJavaVM";

// Sometimes classpath is split into 2 arguments. Compensate for it.
const char* JavaOptionClassPath = "-classpath";

// JNI signature for Java String.
const char* JavaMainMethodSignature = "([Ljava/lang/String;)V";

// class name for NativeInterop
const char* NativeInteropClass = "org/apache/reef/javabridge/NativeInterop";

const int maxPathBufSize = 16 * 1024;
const char ClassPathSeparatorCharForWindows = ';';

// we look for this to delineate java vm arguments from app arguments.
// TODO: be smarter about this. Accomodate arbitrary apps that doesn't 
// contain the term REEFLauncher.
const char* launcherClassSearchStr = "REEFLauncher";

// method to invoke
const char* JavaMainMethodName = "main";

//Prefix for classpath. This is how we tell JNI about the classpath.
const char* JavaClassPath = "-Djava.class.path=";


typedef enum {
    ErrSuccess = 0,
    ErrGetEnvironmentVariable = 1,
    ErrLoadLibraryJVM = 2,
    ErrGetProcAddress = 3,
    ErrCreateJavaVM = 4,
    ErrFindClassEntry = 5,
    ErrGetStaticMethodID = 6,
    ErrFindClassString = 7,
    ErrNewObjectArray = 8,
} ErrorEnum;

// 
// figure out where jvm options end, entry class method and its parameters  begin
//
void GetCounts(
    int cArgs,
    char*argv[],
    int&  optionCount,
    int& firstOptionOrdinal,
    int& argCount,
    int& firstArgOrdinal)
{
    bool option = true;
    optionCount = 0;
    firstOptionOrdinal = 2;
    argCount = 0;
    firstArgOrdinal = -1;

    for (int i = firstOptionOrdinal; i < cArgs; i++) {
        if (option && NULL != strstr(argv[i], launcherClassSearchStr)) {
            option = false;
            firstArgOrdinal = i;
        }
        if (option) {
            ++optionCount;
        }
        else {
            ++argCount;
        }
    }

    if (firstArgOrdinal < 0) {
        throw gcnew ArgumentException("Unable to find a REEF Launcher");
    }
}

//
// Set JVM option. JNI does not support unicode
//
void SetOption(JavaVMOption *option, char *src)
{
    size_t len = strlen(src) + 1;
    char* pszOption = new char[len];
    strcpy_s(pszOption, len, src);
    option->optionString = pszOption;
    option->extraInfo = nullptr;
}

// 
// Jni does not expand * and *.jar 
// We need to do it ourselves.
// 
char *ExpandJarPaths(char *jarPaths)
{
    String^ classPathSeparatorStringForWindows = L";";
    const char classPathSeparatorCharForrWindows = ';';
    String^ jarExtension = L".jar";
    String^ anyJarExtension = L"*.jar";
    String^ asterisk = L"*";

    const int StringBuilderInitalSize = 1024 * 16;
    System::Text::StringBuilder^ sb = gcnew System::Text::StringBuilder(StringBuilderInitalSize);
    sb->Append(gcnew String(JavaClassPath));
    String^ pathString = gcnew String(jarPaths);
    array<String^>^ rawPaths = pathString->Split(classPathSeparatorCharForrWindows);
    for (int i = 0; i < rawPaths->Length; i++)
    {
        String^ oldPath = rawPaths[i];
        int oldPathLength = oldPath->Length;
        String^ path;
        bool shouldExpand = false;
        if (oldPath->EndsWith(asterisk))
        {
            path = oldPath + jarExtension;
            shouldExpand = true;
        }
        else if (oldPath->EndsWith(anyJarExtension))
        {
            path = oldPath;
            shouldExpand = true;
        }
        else
        {
            sb->Append(classPathSeparatorStringForWindows);
            sb->Append(oldPath);
        }
        if (shouldExpand)
        {
            try
            {
                auto filesDir = System::IO::Path::GetDirectoryName(path);
                auto fileName = System::IO::Path::GetFileName(path);
                auto directoryInfo = gcnew System::IO::DirectoryInfo(filesDir);
                auto files = directoryInfo->GetFiles(fileName);
                for (int i = 0; i < files->Length; i++)
                {
                    auto fullName = System::IO::Path::Combine(files[i]->Directory->ToString(), files[i]->ToString());
                    sb->Append(classPathSeparatorStringForWindows);
                    sb->Append(fullName);
                }
            }
            catch (System::IO::DirectoryNotFoundException^)
            {
                //Ignore invalid paths.
            }
        }
    }
    auto newPaths = sb->ToString();
    int len = newPaths->Length;
    char* finalPath = new char[len + 1];
    auto hglobal = System::Runtime::InteropServices::Marshal::StringToHGlobalAnsi(newPaths);
    memcpy(finalPath, (void*)hglobal, len + 1);
    System::Runtime::InteropServices::Marshal::FreeHGlobal(hglobal);
    return finalPath;
}

JavaVMOption* GetJavaOptions(char *argv[], int& optionCount, int firstOptionOrdinal)
{
    JavaVMOption* options = new JavaVMOption[optionCount + 1];
    char classPathBuf[maxPathBufSize];
    int sourceOrdinal = firstOptionOrdinal;

    for (int i = 0; i < optionCount; i++) {
        SetOption(options + i, argv[sourceOrdinal]);
        if (0 == strcmp(argv[i + firstOptionOrdinal], JavaOptionClassPath) && ((i + 1) < optionCount)) {
            strcpy_s(classPathBuf, argv[++sourceOrdinal]);
            for (char* ptr = classPathBuf; *ptr; ptr++) {
                if (*ptr == '/') {
                    *ptr = '\\';
                }
            }
            strcat_s(classPathBuf, ";local\\*;global\\*");
            auto expandedPath = ExpandJarPaths(classPathBuf);
            SetOption(options + i, expandedPath);
            --optionCount;
        }
        ++sourceOrdinal;
    }

    return options;
}

int Get_CreateJavaVM_Function(JNI_CreateJavaVM_FN& fn_JNI_CreateJavaVM)
{
    wchar_t javaHomePath[maxPathBufSize];
    wchar_t jvmDllPath[maxPathBufSize];

    DWORD rc = GetEnvironmentVariable(JAVA_HOME, javaHomePath, maxPathBufSize);
    if (0 == rc) {
        wprintf(L"Could not GetEnvironmentVariable %ls\n", JAVA_HOME);
        return ErrGetEnvironmentVariable;
    }

    // Try all possible dll paths
    HMODULE jvm_dll = NULL;
    for (int i = 0; i < JVM_DLL_PATHS_SIZE; i++) {
        swprintf(jvmDllPath, maxPathBufSize, L"%ls%ls", javaHomePath, JVM_DLL_PATHS[i]);

        jvm_dll = LoadLibrary(jvmDllPath);
        if (jvm_dll == NULL) {
            wprintf(L"Could not load dll %ls\n", jvmDllPath);
        }
        else
        {
            break;
        }
    }

    if (jvm_dll == NULL) {
        return ErrLoadLibraryJVM;
    }

    fn_JNI_CreateJavaVM = (JNI_CreateJavaVM_FN)GetProcAddress(jvm_dll, JNI_CreateJavaVM_Func_Name);
    if (fn_JNI_CreateJavaVM == NULL) {
        printf("Could not GetProcAddress %s\n", JNI_CreateJavaVM_Func_Name);
        return ErrGetProcAddress;
    }

    return ErrSuccess;
}

//
// Creates Java vm with the given options
//
int CreateJVM(JNIEnv*& env, JavaVM*& jvm, JavaVMOption* options, int optionCount) {
    JNI_CreateJavaVM_FN fn_JNI_CreateJavaVM;
    int failureCode = 0;
    if ((failureCode = Get_CreateJavaVM_Function(fn_JNI_CreateJavaVM)) != ErrSuccess) {
        return failureCode;
    }

    for (int i = 0; i < optionCount; i++) {
        printf("Option %d [%s]\n", i, options[i].optionString);
    }
    fflush(stdout);
    JavaVMInitArgs vm_args;
    memset(&vm_args, 0, sizeof(vm_args));
    vm_args.version = JNI_VERSION_1_6;
    vm_args.nOptions = optionCount;
    vm_args.options = options;
    vm_args.ignoreUnrecognized = JNI_FALSE;
    long status = fn_JNI_CreateJavaVM(&jvm, (void**)&env, &vm_args);
    if (status) {
        printf("Could not fn_JNI_CreateJavaVM\n");
        return ErrCreateJavaVM;
    }
    return ErrSuccess;
}

//
// Invokes main method of entry class.
// I.E. org/apache/reef/runtime/common/REEFLauncher
//
int CallMainMethodOfEntryClass(
    JNIEnv* env,
    char*   argv[],
    int     firstArgOrdinal,
    int     argCount)
{
    // int the entry class name, Replace '.' with '/' 
    char classBuf[maxPathBufSize];
    strcpy_s(classBuf, argv[firstArgOrdinal]);
    for (char* ptr = classBuf; *ptr; ptr++) {
        if (*ptr == '.') {
            *ptr = '/';
        }
    }

    // Find the entry class
    jclass mainClass = env->FindClass(classBuf);
    if (!mainClass) {
        printf("Failed to find class '%s'", classBuf);
        fflush(stdout);
        if (env->ExceptionOccurred()) {
            env->ExceptionDescribe();
        }
        fflush(stdout);
        return ErrFindClassEntry;
    }

    // find method 'static void main (String[] args)'
    jmethodID mainMethod = env->GetStaticMethodID(mainClass, JavaMainMethodName, JavaMainMethodSignature);
    if (!mainMethod) {
        printf("Failed to find jmethodID of 'main'");
        return ErrGetStaticMethodID;
    }

    // Find string class
    jclass stringClass = env->FindClass("java/lang/String");
    if (!stringClass) {
        printf("Failed to find java/lang/String");
        return ErrFindClassString;
    }
    stringClass = reinterpret_cast<jclass>(env->NewGlobalRef(stringClass));

    // Allocate string[] for main method parameter
    jobjectArray args = env->NewObjectArray(argCount - 1, stringClass, 0);
    if (!args) {
        printf("Failed to create args array");
        return ErrNewObjectArray;
    }
    args = reinterpret_cast<jobjectArray>(env->NewGlobalRef(args));

    // Copy parameters for main method
    for (int i = 0; i < argCount - 1; ++i) {
        env->SetObjectArrayElement(args, i, env->NewStringUTF(argv[firstArgOrdinal + 1 + i]));
    }

    // Find the _NativeInterop class
    jclass nativeInteropClass = env->FindClass(NativeInteropClass);
    if (nativeInteropClass) {
        nativeInteropClass = reinterpret_cast<jclass>(env->NewGlobalRef(nativeInteropClass));
        printf("Found class '%s'", NativeInteropClass);
        Java_org_apache_reef_javabridge_NativeInterop_registerNatives(env, nativeInteropClass);
    }
    else {
        printf("Did not find class '%s'", NativeInteropClass);
    }
    fflush(stdout);

    // call main method with parameters
    env->CallStaticVoidMethod(mainClass, mainMethod, args);

    return ErrSuccess;
}

int main(int cArgs, char *argv[])
{
    int  optionCount;
    int  firstOptionOrdinal;
    int  argCount;
    int  firstArgOrdinal;

    GetCounts(cArgs, argv, optionCount, firstOptionOrdinal, argCount, firstArgOrdinal);
    JavaVMOption* options = GetJavaOptions(argv, optionCount, firstOptionOrdinal);

    JNIEnv *env;
    JavaVM *jvm;
    int failureCode = 0;
    if ((failureCode = CreateJVM(env, jvm, options, optionCount)) != ErrSuccess) {
        fflush(stdout);
        return failureCode;
    }

    if ((failureCode = CallMainMethodOfEntryClass(env, argv, firstArgOrdinal, argCount)) != ErrSuccess) {
        fflush(stdout);
        return failureCode;
    }

    // Check for errors. 
    if (env->ExceptionOccurred()) {
        env->ExceptionDescribe();
    }

    // Finally, destroy the JavaVM 
    jvm->DestroyJavaVM();

    return 0;
}