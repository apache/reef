mvn -q -fae -DskipTests clean install eclipse:clean eclipse:eclipse package
cd reef-examples
#mvn -q -fae -DskipTests clean compile eclipse:clean eclipse:eclipse
cd ..
cd reef-distro
mvn -q assembly:assembly
