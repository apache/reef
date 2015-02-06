@REM
@REM Copyright (C) 2013 Microsoft Corporation
@REM
@REM Licensed under the Apache License, Version 2.0 (the "License");
@REM you may not use this file except in compliance with the License.
@REM You may obtain a copy of the License at
@REM
@REM         http://www.apache.org/licenses/LICENSE-2.0
@REM
@REM Unless required by applicable law or agreed to in writing, software
@REM distributed under the License is distributed on an "AS IS" BASIS,
@REM WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@REM See the License for the specific language governing permissions and
@REM limitations under the License.
@REM

@echo off
::
:: Copyright (C) 2013 Microsoft Corporation
::
:: Licensed under the Apache License, Version 2.0 (the "License");
:: you may not use this file except in compliance with the License.
:: You may obtain a copy of the License at
::
::         http:\\www.apache.org\licenses\LICENSE-2.0
::
:: Unless required by applicable law or agreed to in writing, software
:: distributed under the License is distributed on an "AS IS" BASIS,
:: WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
:: See the License for the specific language governing permissions and
:: limitations under the License.
::


:: RUNTIME
set SHADED_JAR=bin\reef-bridge-0.11.0-incubating-SNAPSHOT-shaded.jar

set LOGGING_CONFIG=-Djava.util.logging.config.class=org.apache.reef.util.logging.Config

set CLASSPATH=%HADOOP_HOME%\share\hadoop\hdfs\lib\*;%HADOOP_HOME%\share\hadoop\hdfs\*;%HADOOP_HOME%\share\hadoop\common\*;%HADOOP_HOME%\share\hadoop\common\lib\*;%HADOOP_HOME%\share\hadoop\mapreduce\lib\*;%HADOOP_HOME%\share\hadoop\mapreduce\*;%HADOOP_HOME%\share\hadoop\yarn\*;%HADOOP_HOME%\share\hadoop\yarn\lib\*

set CMD=%JAVA_HOME%\bin\java.exe -cp %HADOOP_HOME%\etc\hadoop;%SHADED_JAR%;%CLASSPATH% %*
::%LOGGING_CONFIG%
echo %CMD%
%CMD%
