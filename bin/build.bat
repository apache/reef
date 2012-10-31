mvn -q -fae -DskipTests clean install eclipse:clean eclipse:eclipse
cd reef-examples
mvn -q -fae -DskipTests clean compile eclipse:clean eclipse:eclipse