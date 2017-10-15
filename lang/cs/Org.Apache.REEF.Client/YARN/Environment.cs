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

namespace Org.Apache.REEF.Client.YARN
{
    /// <summary>
    /// Default environment map keys from YARN.
    /// <a href="http://hadoop.apache.org/docs/r2.6.0/hadoop-yarn/hadoop-yarn-site/WebServicesIntro.html">
    /// Hadoop RM REST API</a> documentation.
    /// </summary>
    public enum Environment
    {
        /**
         * USER
         */
        USER,

        /**
         * LOGNAME
         */
        LOGNAME,

        /**
         * HOME
         */
        HOME,

        /**
         * PWD
         */
        PWD,

        /**
         * PATH
         */
        PATH,

        /**
         * SHELL
         */
        SHELL,

        /**
         * JAVA_HOME
         */
        JAVA_HOME,

        /**
         * CLASSPATH
         */
        CLASSPATH,

        /**
         * APP_CLASSPATH
         */
        APP_CLASSPATH,

        /**
         * HADOOP_CLASSPATH.
         */
        HADOOP_CLASSPATH,

        /**
         * LD_LIBRARY_PATH
         */
        LD_LIBRARY_PATH,

        /**
         * HADOOP_CONF_DIR
         */
        HADOOP_CONF_DIR,

        /**
         * HADOOP_CLIENT_CONF_DIR Final, non-modifiable.
         */
        HADOOP_CLIENT_CONF_DIR,

        /**
         * $HADOOP_COMMON_HOME
         */
        HADOOP_COMMON_HOME,

        /**
         * $HADOOP_HDFS_HOME
         */
        HADOOP_HDFS_HOME,

        /**
         * $MALLOC_ARENA_MAX
         */
        MALLOC_ARENA_MAX,

        /**
         * $HADOOP_YARN_HOME
         */
        HADOOP_YARN_HOME,

        /**
         * $CLASSPATH_PREPEND_DISTCACHE
         * Private, Windows specific
         */
        CLASSPATH_PREPEND_DISTCACHE,

        /**
         * $CONTAINER_ID
         * Exported by NodeManager and non-modifiable by users.
         */
        CONTAINER_ID,

        /**
         * $NM_HOST
         * Exported by NodeManager and non-modifiable by users.
         */
        NM_HOST,

        /**
         * $NM_HTTP_PORT
         * Exported by NodeManager and non-modifiable by users.
         */
        NM_HTTP_PORT,

        /**
         * $NM_PORT
         * Exported by NodeManager and non-modifiable by users.
         */
        NM_PORT,

        /**
         * $LOCAL_DIRS
         * Exported by NodeManager and non-modifiable by users.
         */
        LOCAL_DIRS,

        /**
         * $LOG_DIRS
         * Exported by NodeManager and non-modifiable by users.
         * Comma separate list of directories that the container should use for
         * logging.
         */
        LOG_DIRS
    }
}
