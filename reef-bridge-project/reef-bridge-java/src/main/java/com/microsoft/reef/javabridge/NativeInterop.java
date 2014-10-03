/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.microsoft.reef.javabridge;

import com.microsoft.reef.driver.context.ContextMessage;
import com.microsoft.reef.driver.task.SuspendedTask;
import org.apache.commons.compress.utils.IOUtils;

import java.io.*;
import java.util.Date;
import java.util.HashMap;

public class NativeInterop {

  public static native void loadClrAssembly (String filePath);

  public static native void ClrBufferedLog(int level, String message);

  public static  native long[] CallClrSystemOnStartHandler(String dateTime);

  public static native void ClrSystemAllocatedEvaluatorHandlerOnNext (
      long handle,
      AllocatedEvaluatorBridge javaEvaluatorBridge,
      InteropLogger interopLogger
  );

  public static native void ClrSystemActiveContextHandlerOnNext (
      long handle,
      ActiveContextBridge javaActiveContextBridge,
      InteropLogger interopLogger
  );

  public static native void ClrSystemEvaluatorRequstorHandlerOnNext (
      long handle,
      EvaluatorRequestorBridge javaEvluatorRequstorBridge,
      InteropLogger interopLogger
  );

  public static native void ClrSystemTaskMessageHandlerOnNext (
      long handle,
      byte[] mesage,
      TaskMessageBridge javaTaskMessageBridge,
      InteropLogger interopLogger
    );

  public static native void ClrSystemFailedTaskHandlerOnNext (
      long handle,
      FailedTaskBridge failedTaskBridge,
      InteropLogger interopLogger
    );

  public static native void ClrSystemHttpServerHandlerOnNext(
      long handle,
      HttpServerEventBridge httpServerEventBridge,
      InteropLogger interopLogger
    );

  public static native void ClrSystemFailedEvaluatorHandlerOnNext (
      long handle,
      FailedEvaluatorBridge failedEvaluatorBridge,
      InteropLogger interopLogger
  );

  public static native void ClrSystemCompletedTaskHandlerOnNext (
      long handle,
      CompletedTaskBridge completedTaskBridge,
      InteropLogger interopLogger
  );

  public static native void ClrSystemRunningTaskHandlerOnNext (
      long handle,
      RunningTaskBridge runningTaskBridge,
      InteropLogger interopLogger
  );

  public static native void ClrSystemSupendedTaskHandlerOnNext (
      long handle,
      SuspendedTaskBridge suspendedTaskBridge
  );

  public static native void ClrSystemCompletdEvaluatorHandlerOnNext (
      long handle,
      CompletedEvaluatorBridge completedEvaluatorBridge
  );

  public static native void ClrSystemClosedContextHandlerOnNext (
      long handle,
      ClosedContextBridge closedContextBridge
  );

  public static native void ClrSystemFailedContextHandlerOnNext (
      long handle,
      FailedContextBridge failedContextBridge
  );

  public static native void ClrSystemContextMessageHandlerOnNext (
      long handle,
      ContextMessageBridge contextMessageBridge
  );

  public static native void ClrSystemDriverRestartHandlerOnNext (
      long handle
  );

  public static native void ClrSystemDriverRestartActiveContextHandlerOnNext (
      long handle,
      ActiveContextBridge activeContextBridge
  );

  public static native void ClrSystemDriverRestartRunningTaskHandlerOnNext (
      long handle,
      RunningTaskBridge runningTaskBridge
  );

  private static final String LIB_BIN = "/";
  private static final String DLL_EXTENSION = ".dll";
  private static final String CPP_BRIDGE = "JavaClrBridge";
  private static final String tmpLoadingDirectory = System.getProperty("user.dir") + "/reef/CLRLoadingDirectory";
  private static final String[] managedDlls ={
      "ClrHandler",
      "msvcr110",
  };

  public static final String CLASS_HIERARCHY_FILENAME = "clrClassHierarchy.bin";
  public static final String GLOBAL_LIBRARIES_FILENAME = "userSuppliedGlobalLibraries.txt";

  public static final String EvaluatorRequestorKey = "EvaluatorRequestor";
  public static final String AllocatedEvaluatorKey = "AllocatedEvaluator";
  public static final String ActiveContextKey = "ActiveContext";
  public static final String TaskMessageKey = "TaskMessage";
  public static final String FailedTaskKey = "FailedTask";
  public static final String FailedEvaluatorKey = "FailedEvaluator";
  public static final String HttpServerKey = "HttpServerKey";
  public static final String CompletedTaskKey = "CompletedTask";
  public static final String RunningTaskKey = "RunningTask";
  public static final String SuspendedTaskKey = "SuspendedTask";
  public static final String CompletedEvaluatorKey = "CompletedEvaluator";
  public static final String ClosedContextKey= "ClosedContext";
  public static final String FailedContextKey = "FailedContext";
  public static final String ContextMessageKey = "ContextMessage";
  public static final String DriverRestartKey = "DriverRestart";
  public static final String DriverRestartActiveContextKey = "DriverRestartActiveContext";
  public static final String DriverRestartRunningTaskKey = "DriverRestartRunningTask";


  public static final int nHandlers = 17;
  public static final HashMap<String,Integer> Handlers = new HashMap<String, Integer>(){{
    put(EvaluatorRequestorKey, 0);
    put(AllocatedEvaluatorKey, 1);
    put(ActiveContextKey, 2);
    put(TaskMessageKey, 3);
    put(FailedTaskKey,4);
    put(FailedEvaluatorKey, 5);
    put(HttpServerKey, 6);
    put(CompletedTaskKey, 7);
    put(RunningTaskKey, 8);
    put(SuspendedTaskKey, 9);
    put(CompletedEvaluatorKey, 10);
    put(ClosedContextKey, 11);
    put(FailedContextKey, 12);
    put(ContextMessageKey, 13);
    put(DriverRestartKey, 14);
    put(DriverRestartActiveContextKey, 15);
    put(DriverRestartRunningTaskKey, 16);
    }
  };

  static {
    System.out.println("============== Driver Bridge initiated, loading DLLs at time " + new Date().toString() + "============== ");
    new File(tmpLoadingDirectory).mkdir();
    loadFromJar();
    System.out.println("================== Done loading dlls for Driver at time " + new Date().toString() + " ================== \n");
  }

  private static void loadFromJar() {
  // we need to put both DLLs to temp dir
    loadLib(CPP_BRIDGE, false);
    final File[]  files =  new File(System.getProperty("user.dir") + "/reef/global").listFiles(new FilenameFilter() {
      public boolean accept(File dir, String name) {
        return name.toLowerCase().endsWith(DLL_EXTENSION);
      }
    });
    //System.out.println("Total dll files to load from " + System.getProperty("user.dir") + "/reef/global" + "  are: " + files.length );

    for (int i=0; i<files.length; i++)
    {
      try {
        final String fileName = files[i].getName();
        String fileNameWithoutExtension =  fileName;
        if (fileName.indexOf(".") > 0) {
          fileNameWithoutExtension = fileName.substring(0, fileName.lastIndexOf("."));
        }
        loadLib(fileNameWithoutExtension, true);
      } catch (final Exception e) {
        System.out.println("exception lading dll library " + e);
        throw e;
      }
    }

    for (int i=0; i<managedDlls.length; i++)
    {
      loadLib(managedDlls[i], true);
    }
  }

  /**
   * Puts library to temp dir and loads to memory
   */

  private static void loadLib(String name, final boolean copyOnly) {
    name = name + DLL_EXTENSION;
    //System.out.println("LOADING " + System.getProperty("user.dir") + "/reef/global/" + name );

    try {
      final String path = "/ReefDriverAppDlls/" + name;
      //System.out.println("trying to load: " +  NativeInterop.class.getClass().getResource(path).getPath());
      final InputStream in = NativeInterop.class.getResourceAsStream(path);
      // always write to different location
      final File fileOut = new File(tmpLoadingDirectory + LIB_BIN + name);
      final OutputStream out = new FileOutputStream(fileOut);
      //System.out.println("after new FileOutputStream(fileOut)");
      if (null == in)
      {
        // System.out.println("Cannot find " + path);
        return;
      }
      if (out == null)
      {
        System.out.println("** out is null");
      }

      IOUtils.copy(in, out);
      in.close();
      out.close();

      if (false == copyOnly)
      {
        //System.out.println("Loading DLL not copyonly");
        System.load(fileOut.toString());
        //System.out.println("Loading DLL not copyonly done");
      }
      else
      {
        //System.out.println("Loading DLL copyonly");
          if (null == fileOut)
          {
            System.out.println("fileOut is NULL");
          }
           //System.out.println("fileOut.toString() " + fileOut.toString());
           NativeInterop.loadClrAssembly (fileOut.toString());
           //System.out.println("Done Loading DLL " +  fileOut.toString());
           }
        } catch (final Exception e) {
            throw new UnsatisfiedLinkError("Failed to load required DLL " +  name);
        }
    }
}
