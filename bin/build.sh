#!/bin/bash


cd ${REEF_HOME}
mvn clean install package -DskipTests
cd reef-distro
mvn assembly:assembly
