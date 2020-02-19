# Milvus Java SDK

[![Maven Central](https://img.shields.io/maven-central/v/io.milvus/milvus-sdk-java.svg)](https://search.maven.org/artifact/io.milvus/milvus-sdk-java/)

Java SDK for [Milvus](https://github.com/milvus-io/milvus). To contribute code to this project, please read our [contribution guidelines](https://github.com/milvus-io/milvus/blob/master/CONTRIBUTING.md) first.

## Getting started

### Prerequisites

- Java 8 or higher.
- Apache Maven or Gradle/Grails

### Install Java SDK

1. Clone this repository to your local machine. The following table shows Milvus versions and compatible SDK versions.

    |Milvus version| SDK version|
    |:-----:|:-----:|
    | 0.5.0 | 0.2.2 | 
    | 0.5.1 | 0.2.2 | 
    | 0.5.2 | 0.2.2 | 
    | 0.5.3 | 0.3.0 | 
    | 0.6.0 | 0.4.1 | 

2. Use **Apache Maven** or **Gradle**/**Grails** to compile the SDK. For Apache Maven, add the following dependency:

    ```xml
    <dependency>
        <groupId>io.milvus</groupId>
        <artifactId>milvus-sdk-java</artifactId>
        <version>0.4.1</version>
    </dependency>
    ```

    For Gradle/Grails, use the following command:

    ```shell
    compile 'io.milvus:milvus-sdk-java:0.4.1'
    ```

### Examples

Please refer to [examples](https://github.com/milvus-io/milvus-sdk-java/tree/master/examples) folder for Java example programs.

### Documentation

- [Javadoc](https://milvus-io.github.io/milvus-sdk-java/javadoc/index.html)

### Additional information

- The Java source code is formatted using [google-java-format](https://github.com/google/google-java-format).
