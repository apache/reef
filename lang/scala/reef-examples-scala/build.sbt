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

name         := "REEF-on-Spark"
version      := "1.02"
organization := "org.apache.reef"

scalaVersion := "2.11.8"

publishMavenStyle := true

val sparkVersion = "2.1.0"
val reefVersion  = "0.16.0-SNAPSHOT"

resolvers += Resolver.mavenLocal

libraryDependencies += "com.jsuereth" %% "scala-arm" % "2.0" // for AutoCloseable stuff

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"

libraryDependencies += "org.apache.reef" % "reef-common"        % reefVersion
libraryDependencies += "org.apache.reef" % "reef-runtime-local" % reefVersion
libraryDependencies += "org.apache.reef" % "reef-runtime-yarn"  % reefVersion
libraryDependencies += "org.apache.reef" % "reef-io"            % reefVersion

javaOptions in (Test,run) += "-Djava.util.logging.config.class=org.apache.reef.util.logging.Config"
