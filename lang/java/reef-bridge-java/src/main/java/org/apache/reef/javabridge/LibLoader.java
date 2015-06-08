/*
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

import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.util.logging.LoggingScope;
import org.apache.reef.util.logging.LoggingScopeFactory;

import javax.inject.Inject;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Loading CLR libraries.
 */
public final class LibLoader {

  private static final Logger LOG = Logger.getLogger(LibLoader.class.getName());

  private static final String DLL_EXTENSION = ".dll";

  private final LoggingScopeFactory loggingScopeFactory;

  private final REEFFileNames reefFileNames;

  @Inject
  private LibLoader(final LoggingScopeFactory loggingScopeFactory, final REEFFileNames reefFileNames) {
    this.loggingScopeFactory = loggingScopeFactory;
    this.reefFileNames = reefFileNames;
  }

  /**
   * Load CLR libraries.
   */
  public void loadLib() throws IOException {
    LOG.log(Level.INFO, "Loading DLLs for driver at time {0}." + new Date().toString());
    try (final LoggingScope lb = loggingScopeFactory.loadLib()) {

      // Load the native library connecting C# and Java
      this.loadBridgeDLL();

      // Load all DLLs in local
      loadAllManagedDLLs(this.reefFileNames.getLocalFolder());

      // Load all DLLs in global
      loadAllManagedDLLs(this.reefFileNames.getGlobalFolder());
    }
    LOG.log(Level.INFO, "Done loading DLLs for Driver at time {0}." + new Date().toString());
  }

  /**
   * Loads the Bridge DLL.
   * <p>
   * If the file is found in the reef/local folder, it is used. Else, we load the one in reef/global. If that isn't
   * present, this method throws an IOException
   *
   * @throws FileNotFoundException if neither file is available.
   * @throws Throwable             if the DLL is found, but System.load() throws an exception.
   */
  private void loadBridgeDLL() throws FileNotFoundException {
    final File bridgeDLLFile = this.getBridgeDllFile();
    LOG.log(Level.FINEST, "Attempting to load the bridge DLL from {0}", bridgeDLLFile);
    System.load(bridgeDLLFile.getAbsolutePath());
    LOG.log(Level.INFO, "Loaded the bridge DLL from {0}", bridgeDLLFile);
  }

  /**
   * Returns the File holding the bridge DLL.
   * <p>
   * This method prefers the one in reef/local. If that isn't found, the one in reef/global is used. If neither exists,
   * a FileNotFoundException is thrown.
   *
   * @throws FileNotFoundException if neither file is available.
   */
  private File getBridgeDllFile() throws FileNotFoundException {
    final File bridgeDllInLocal = reefFileNames.getBridgeDLLInLocalFolderFile();
    if (bridgeDllInLocal.exists()) {
      return bridgeDllInLocal;
    } else {
      final File bridgeDllInGlobal = reefFileNames.getBridgeDLLInGlobalFolderFile();
      if (bridgeDllInGlobal.exists()) {
        return bridgeDllInGlobal;
      }
    }
    // If we got here, neither file exists.
    throw new FileNotFoundException("Couldn't find the bridge DLL in the local or global folder.");
  }

  /**
   * Loads all managed DLLs found in the given folder.
   *
   * @param folder
   */
  private static void loadAllManagedDLLs(final File folder) {
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
  private static void loadManagedDLL(final File dllFile) {
    final String absolutePath = dllFile.getAbsolutePath();
    try {
      LOG.log(Level.FINE, "Loading Managed DLL {0} ", absolutePath);
      NativeInterop.loadClrAssembly(absolutePath);
    } catch (final Exception e) {
      LOG.log(Level.SEVERE, "Unable to load managed DLL {0}", absolutePath);
      throw e;
    }
  }
}
