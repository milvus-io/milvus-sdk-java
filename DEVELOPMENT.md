# Development
This document will help to setup your development environment and running tests for milvus java sdk. If you encounter a problem, please file an issue.

## Getting started

### Prerequisites
    -   Java 8 or higher
    -   Apache Maven

### Clone the code

```shell
$ git clone --recursive git@github.com:milvus-io/milvus-sdk-java.git
```

Milvus proto files are managed by a submodule project under the directory: sdk-core/src/main/milvus-proto
Fetch Milvus proto files by the following command(If the previous clone is not with submodules)
```shell
$ git submodule update --init
```

If you are using Idea, go to Project Root in Idea, right click on `milvus-sdk-java` and select `Maven` -> `Reload Project`

## Building Milvus java SDK

Call the following command to generate protobuf related code
```shell
$  mvn install
```

### Unit Tests
Unit tests are under sdk-core/src/test and sdk-bulkwriter/src/test

## GitHub Flow
Milvus SDK repo follows the same git work flow as milvus main repo, see
https://milvus.io/community/contributing_to_milvus.md

If you have any questions about how to fork, clone, create branch, commit, push, open a pull request,
please see https://github.com/firstcontributions/first-contributions