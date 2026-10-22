# Milvus Spring Boot Starter

Spring Boot auto-configuration for the Milvus Java SDK. It creates a `MilvusClientV2` bean from `milvus.*`
properties, so you can inject the client instead of building it by hand.

Requirements: Java 17+ and Spring Boot 3.x or 4.x.

## Usage

Add the starter to your project:

```xml
<dependency>
    <groupId>io.milvus</groupId>
    <artifactId>milvus-sdk-java-spring-boot-starter</artifactId>
    <version>${milvus-sdk-java.version}</version>
</dependency>
```

Configure the connection in `application.yml`:

```yaml
milvus:
  uri: http://localhost:19530
  token: root:Milvus
  db-name: default
```

Then inject the client:

```java
@Service
public class SearchService {

    private final MilvusClientV2 milvusClient;

    public SearchService(MilvusClientV2 milvusClient) {
        this.milvusClient = milvusClient;
    }
}
```

The client is closed when the application context shuts down.

### Connecting at startup

Like the SDK itself, the client connects when it is created, so by default the application fails to start if
the Milvus server is unreachable. To connect on first use instead:

- inject the client lazily, with `@Lazy MilvusClientV2` or `ObjectProvider<MilvusClientV2>`, or
- enable lazy initialization for the whole application with `spring.main.lazy-initialization=true`.

To turn the auto-configuration off, for example on a machine without a Milvus server, set `milvus.enabled=false`.

## Properties

Properties that are not set, or set to an empty value, keep the defaults of `ConnectConfig`. The exception is
`milvus.uri`: `ConnectConfig` has no default URI, so the starter defaults to a local Milvus standalone server.

| Property | Description | Default |
|---|---|---|
| `milvus.enabled` | Enable the auto-configuration | `true` |
| `milvus.uri` | URI of the Milvus server; must not be empty | `http://localhost:19530` |
| `milvus.token` | Authentication token, e.g. `root:Milvus` or a Zilliz Cloud API key | |
| `milvus.username` / `milvus.password` | Credentials for authentication | |
| `milvus.db-name` | Database to connect to | |
| `milvus.connect-timeout` | Timeout for establishing the connection | `10s` |
| `milvus.keep-alive-time` | Interval between keep-alive pings | `10s` |
| `milvus.keep-alive-timeout` | Time to wait for a keep-alive acknowledgement | `5s` |
| `milvus.keep-alive-without-calls` | Send keep-alive pings when there are no outstanding calls | `true` |
| `milvus.rpc-deadline` | Deadline for each RPC call, `0` disables it | `0` |
| `milvus.idle-timeout` | Idle time after which the connection is closed | `24h` |
| `milvus.secure` | Use a TLS connection | `false` |
| `milvus.server-name` | Expected server name for TLS verification | |
| `milvus.server-pem-path` | Server certificate for one-way TLS | |
| `milvus.ca-pem-path` / `milvus.client-pem-path` / `milvus.client-key-path` | Certificates and key for mutual TLS | |
| `milvus.proxy-address` | Proxy to connect through | |
| `milvus.enable-precheck` | Check that the server is ready when connecting | `false` |

## Customization

To set options that have no property, such as an `SSLContext` or telemetry settings, declare one or more
`MilvusConnectConfigBuilderCustomizer` beans. They run after the properties are applied, in `@Order` order:

```java
@Bean
MilvusConnectConfigBuilderCustomizer milvusSslCustomizer(SSLContext sslContext) {
    return builder -> builder.sslContext(sslContext);
}
```

To take full control, define your own `ConnectConfig` or `MilvusClientV2` bean; the auto-configured one backs off.
