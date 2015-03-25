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

package org.apache.reef.javabridge;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.util.logging.LoggingScope;
import org.apache.reef.util.logging.LoggingScopeFactory;

import javax.inject.Inject;
import java.io.*;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Loading CLR libraries
 */
public class LibLoader {

  private static final Logger LOG = Logger.getLogger(LibLoader.class.getName());

  private static final String LIB_BIN = "/";
  private static final String DLL_EXTENSION = ".dll";
  private static final String USER_DIR = "user.dir";

  private final LoggingScopeFactory loggingScopeFactory;

  private final REEFFileNames reefFileNames;

  @Inject
  private LibLoader(final LoggingScopeFactory loggingScopeFactory, final REEFFileNames reefFileNames) {
    this.loggingScopeFactory = loggingScopeFactory;
    this.reefFileNames = reefFileNames;
  }

  /**
   * Load CLR libraries
   */
  public void loadLib() throws IOException {
    LOG.log(Level.INFO, "Loading DLLs for driver at time {0}." + new Date().toString());
    try (final LoggingScope lb = loggingScopeFactory.loadLib()) {

      // Load the native library connecting C# and Java
      this.loadBridgeDLL();

      // Load all DLLs in local
      this.loadAllManagedDLLs(this.reefFileNames.getLocalFolder());

      // Load all DLLs in global
      this.loadAllManagedDLLs(this.reefFileNames.getGlobalFolder());
    }
    LOG.log(Level.INFO, "Done loading DLLs for Driver at time {0}." + new Date().toString());
  }

  /**
   * Loads the Bridge DLL. First, it attempts to load from the reef/local folder. Second attempt is reef/global, last
   * attempt is loading it from the JAR.
   *
   * @throws IOException If all attempts fail.
   */
  private void loadBridgeDLL() throws IOException {
    try {
      loadBridgeDLLFromLocal();
    } catch (final Throwable t) {
      try {
        loadBridgeDLLFromGlobal();
      } catch (final Throwable t2) {
        loadBridgeDLLFromJAR();
      }
    }
  }


  /**
   * Attempts to load the bridge DLL from the global folder.
   */
  private void loadBridgeDLLFromGlobal() {
    LOG.log(Level.INFO, "Attempting to load the bridge DLL from the global folder.");
    loadBridgeDLLFromFile(reefFileNames.getBridgeDLLInGlobalFolderFile());
  }

  /**
   * Attempts to load the bridge DLL from the local folder.
   */
  private void loadBridgeDLLFromLocal() {
    LOG.log(Level.INFO, "Attempting to load the bridge DLL from the local folder.");
    loadBridgeDLLFromFile(reefFileNames.getBridgeDLLInLocalFolderFile());
  }

  /**
   * Attempts to load the bridge DLL from the given file.
   *
   * @param bridgeDLLFile
   */
  private static void loadBridgeDLLFromFile(final File bridgeDLLFile) {
    LOG.log(Level.INFO, "Attempting to load the bridge DLL from {0}", bridgeDLLFile);
    System.load(bridgeDLLFile.getAbsolutePath());
    LOG.log(Level.INFO, "Successfully loaded the bridge DLL from {0}", bridgeDLLFile);
  }

  /**
   * Attempts to load the bridge DLL from the JAR file.
   *
   * @throws IOException
   * @deprecated We should use the files instead.
   */
  @Deprecated
  private void loadBridgeDLLFromJAR() throws IOException {
    final String tempLoadDir = System.getProperty(USER_DIR) + this.reefFileNames.getLoadDir();
    new File(tempLoadDir).mkdir();
    LOG.log(Level.INFO, "loadBridgeDLL() - tempLoadDir created: {0} ", tempLoadDir);
    final String bridgeMixedDLLName = this.reefFileNames.getBridgeDLLName();
    LOG.log(Level.INFO, "loadBridgeDLL() - BridgeMixedDLLName: {0}", bridgeMixedDLLName);
    loadFromReefJar(bridgeMixedDLLName, false);
  }

  /**
   * Loads all managed DLLs found in the given folder.
   *
   * @param folder
   */
  private void loadAllManagedDLLs(final File folder) {
    LOG.log(Level.INFO, "Loading all managed DLLs from {0}", folder.getAbsolutePath());
    final File[] files = folder.listFiles(new FilenameFilter() {
      public boolean accept(File dir, String name) {
        return name.toLowerCase().endsWith(DLL_EXTENSION);
      }
    });

    for (final File f : files) {
      loadManagedDLL(f);
    }
  }

  /**
   * Loads the given DLL.
   *
   * @param dllFile
   */
  private void loadManagedDLL(final File dllFile) {
    final String absolutePath = dllFile.getAbsolutePath();
    try {
      LOG.log(Level.FINE, "Loading Managed DLL {0} ", absolutePath);
      NativeInterop.loadClrAssembly(absolutePath);
    } catch (final Exception e) {
      LOG.log(Level.SEVERE, "Unable to load managed DLL {0}", absolutePath);
      throw e;
    }
  }

  /**
   * Get file from jar file and copy it to temp dir and loads the library to memory.
   *
   * @deprecated This is replaced by loading it from the folders directly.
   */
  @Deprecated
  private void loadFromReefJar(String name, final boolean managed) throws IOException {
    LOG.log(Level.SEVERE, "Consider upgrading your REEF client. Loading DLLs from the JAR is deprecated.");
    if (!name.endsWith(".dll")) {
      name = name + DLL_EXTENSION;
    }
    try {
      File fileOut = null;
      // get input file from jar
      final String path = this.reefFileNames.getReefDriverAppDllDir() + name;
      LOG.log(Level.INFO, "Source file path: " + path);
      final java.net.URL url = NativeInterop.class.getClass().getResource(path);
      if (url != null) {
        LOG.log(Level.INFO, "Source file: " + url.getPath());
      }
      try (final InputStream in = NativeInterop.class.getResourceAsStream(path)) {
        //copy to /reef/CLRLoadingDirectory
        final String tempLoadDir = System.getProperty(USER_DIR) + this.reefFileNames.getLoadDir();
        fileOut = new File(tempLoadDir + LIB_BIN + name);
        LOG.log(Level.INFO, "Destination file: " + fileOut.toString());
        if (null == in) {
          LOG.log(Level.WARNING, "Cannot find " + path);
          return;
        }
        try (final OutputStream out = new FileOutputStream(fileOut)) {
          IOUtils.copy(in, out);
        }
      }
      loadAssembly(fileOut, managed);
    } catch (final FileNotFoundException e) {
      LOG.log(Level.SEVERE, "File not find exception: ", name);
      throw e;
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "File copy error: ", name);
      throw e;
    }
  }

  /**
   * load assembly
   *
   * @param fileOut
   * @param managed
   */
  private void loadAssembly(final File fileOut, final boolean managed) {
    if (managed) {
      NativeInterop.loadClrAssembly(fileOut.toString());
      LOG.log(Level.INFO, "Loading DLL managed done");
    } else {
      System.load(fileOut.toString());
      LOG.log(Level.INFO, "Loading DLL not managed done");
    }
  }
}