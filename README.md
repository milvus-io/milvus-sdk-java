# Milvus Java SDK

[![Maven Central](https://img.shields.io/maven-central/v/io.milvus/milvus-sdk-java.svg)](https://search.maven.org/artifact/io.milvus/milvus-sdk-java/)

Java SDK for [Milvus](https://github.com/milvus-io/milvus). To contribute to this project, please read our [contribution guidelines](https://github.com/milvus-io/milvus/blob/1.1/CONTRIBUTING.md) first.

## Getting started

### Prerequisites

    -   Java 8 or higher
    -   Apache Maven or Gradle/Grails

The following table shows compatibilities between Milvus and Java SDK.

| Milvus version | Java SDK version |
| :------------: | :--------------: |
|     1.0.x      |    1.0.0         |
|     1.1.x      |    1.1.1         |

### Install Java SDK

You can use **Apache Maven** or **Gradle**/**Grails** to download the SDK.

   - Apache Maven

       ```xml
        <dependency>
            <groupId>io.milvus</groupId>
            <artifactId>milvus-sdk-java</artifactId>
            <version>1.1.1</version>
        </dependency>
       ```

   - Gradle/Grails

        ```gradle
        compile 'io.milvus:milvus-sdk-java:1.1.1'
        ```

### Examples

Please refer to [examples](https://github.com/milvus-io/milvus-sdk-java/tree/1.1/examples) folder for Java SDK examples.

### Documentation

- [Javadoc](https://milvus-io.github.io/milvus-sdk-java/javadoc/index.html)

### Troubleshooting

- If you encounter the following error when running your application:
    ```
    Exception in thread "main" java.lang.NoClassDefFoundError: org/slf4j/LoggerFactory
    ```
  This is because SLF4J jar files need to be added into your application's classpath. SLF4J is required by Java SDK for logging purpose.
  
  To fix this issue, you can use **Apache Maven** or **Gradle**/**Grails** to download the required jar files.
                                                                                                         
    - Apache Maven
    
        ```xml
         <dependency>
             <groupId>org.slf4j</groupId>
             <artifactId>slf4j-api</artifactId>
             <version>1.7.30</version>
         </dependency>
        ```
    
    - Gradle/Grails
    
         ```gradle
         compile group: 'org.slf4j', name: 'slf4j-api', version: '1.7.30'
         ```
