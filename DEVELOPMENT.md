# Development
This document will help to setup your development environment and running tests for milvus java sdk. If you encounter a problem, please file an issue.

## Getting started

### Prerequisites
    -   Java 8 or higher
    -   Apache Maven

## Building Milvus java SDK
call the following command to generate protobuf related code
```shell
  mvn install
```

## Building Milvus
see detailed information at:
https://github.com/milvus-io/milvus/blob/master/DEVELOPMENT.md

## Start a Milvus cluster
You need to start a latest milvus cluster to test the java SDK, see instructions at:
https://milvus.io/docs/v2.0.0/install_standalone-docker.md

### Unit Tests
All unit test is under director src/test, TBD

## GitHub Flow
Milvus SDK repo follows the same git work flow as milvus main repo, see
https://milvus.io/community/contributing_to_milvus.md

If you have any questions about how to fork, clone, create branch, commit, push, open a pull request,
please see https://github.com/firstcontributions/first-contributions