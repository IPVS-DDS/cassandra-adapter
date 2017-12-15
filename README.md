# cassandra-adapter

This project is intended to be packaged and executed as a Docker container.


To build the project, simply run

```sh
mvn package
```

This compiles the java sources, creates a Docker container including the resulting jar and all its dependencies and registers the container with the local docker registry.
**NOTE**: You'll need to have the Docker service running for this to succeed.
See [spotify/dockerfile-maven](https://github.com/spotify/dockerfile-maven) for more information.


After the Docker container is registered, it can be run:

```sh
docker run --network=host dds/cassandra-adapter:1.0-SNAPSHOT [zookeeperURLs] [schemaRegistryURL]
```


<!-- To be determined -->
