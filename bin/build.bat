mvn -fae -DskipTests clean install eclipse:clean eclipse:eclipse
cd reef-examples
mvn -fae -DskipTests clean compile eclipse:clean eclipse:eclipse