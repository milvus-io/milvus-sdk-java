# Single-Proxy running-request tests

`RunningRequestsTest` covers native Java List/Cancel: empty List and unknown ID,
empty/null IDs rejected without an RPC, and Search cancellation with server code
3002, immediate List cleanup, NQ/topK metadata, and a successful control Search.
The three tests join the existing TestNG `Smoke` group. They create and drop only
their own collection and skip environments with more than one Proxy.

These APIs are not in the published 3.0.10 SDK previously used by this test project.
Build and install the SDK from this checkout first; a published jar with the same
version does not necessarily contain the unreleased APIs.

From the repository root (JDK 17+, Maven):

```sh
git submodule update --init
mvn -pl sdk-core -am -Dmaven.test.skip=true install
mvn -f tests/milvustestv2/pom.xml -Dtest=RunningRequestsTest \
  -Duri=http://localhost:19530 -Dtoken=root:Milvus test
```

Use a standalone deployment or a distributed deployment with exactly one Proxy.
The Search case prepares 100,000 128-dimensional vectors with a FLAT/L2 index;
its 16,384-query target must still be running when Cancel reaches it. A missed
cancellation window fails the test instead of skipping it. No cleanup polling
or server failpoints are used.
