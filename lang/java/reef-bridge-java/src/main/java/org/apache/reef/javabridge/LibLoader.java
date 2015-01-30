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
  private static final String[] MANAGED_DLLS = {
      "ClrHandler",
      "msvcr110",
  };

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
      final String tempLoadDir = System.getProperty(USER_DIR) + this.reefFileNames.getLoadDir();
      LOG.log(Level.INFO, "load Folder: " + tempLoadDir);
      new File(tempLoadDir).mkdir();

      loadFromReefJar(this.reefFileNames.getCppBridge(), false);

      loadLibFromGlobal();

      for (int i = 0; i < MANAGED_DLLS.length; i++) {
        loadFromReefJar(MANAGED_DLLS[i], true);
      }
    }
    LOG.log(Level.INFO, "Done loading DLLs for Driver at time {0}." + new Date().toString());
  }

  /**
   * Load assemblies at global folder
   */
  private void loadLibFromGlobal() {
    final String globalFilePath = System.getProperty(USER_DIR) + this.reefFileNames.getReefGlobal();
    final File[] files = new File(globalFilePath).listFiles(new FilenameFilter() {
      public boolean accept(File dir, String name) {
        return name.toLowerCase().endsWith(DLL_EXTENSION);
      }
    });

    LOG.log(Level.INFO, "Total dll files to load from {0} is {1}.", new Object[] {globalFilePath, files.length} );
    for (int i = 0; i < files.length; i++) {
      try {
        LOG.log(Level.INFO, "file to load : " + files[i].toString());
        NativeInterop.loadClrAssembly(files[i].toString());
      } catch (final Exception e) {
        LOG.log(Level.SEVERE, "exception in loading dll library: ", files[i].toString());
        throw e;
      }
    }
  }

  /**
   * Get file from jar file and copy it to temp dir and loads the library to memory
  **/
  private void loadFromReefJar(String name, final boolean managed) throws IOException {

    name = name + DLL_EXTENSION;
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
        try (final OutputStream out = new FileOutputStream(fileOut) ) {
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
