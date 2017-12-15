FROM openjdk:8

ENTRYPOINT ["/usr/bin/java", "-jar", "/usr/share/cassandra-adapter/cassandra-adapter.jar"]

# Add Maven dependencies
ADD target/lib /usr/share/cassandra-adapter/lib
ARG JAR_FILE
ADD target/${JAR_FILE} /usr/share/cassandra-adapter/cassandra-adapter.jar
