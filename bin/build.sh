#!/bin/bash


cd ${REEF_HOME}
mvn clean install -DskipTests
cd reef-distro
mvn assembly:assembly
