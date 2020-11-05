package io.milvus.client;

import io.grpc.Attributes;
import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;
import io.grpc.NameResolverProvider;
import java.net.SocketAddress;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class StaticNameResolverProvider extends NameResolverProvider {
  private final List<SocketAddress> addresses;

  public StaticNameResolverProvider(SocketAddress... addresses) {
    this.addresses = Arrays.asList(addresses);
  }

  @Override
  public String getDefaultScheme() {
    return "static";
  }

  @Override
  protected boolean isAvailable() {
    return true;
  }

  @Override
  protected int priority() {
    return 0;
  }

  @Override
  public NameResolver newNameResolver(URI targetUri, NameResolver.Args args) {
    if (!getDefaultScheme().equals(targetUri.getScheme())) {
      return null;
    }
    return new NameResolver() {
      @Override
      public String getServiceAuthority() {
        return "localhost";
      }

      @Override
      public void start(Listener2 listener) {
        List<EquivalentAddressGroup> addrs =
            addresses.stream()
                .map(addr -> new EquivalentAddressGroup(Collections.singletonList(addr)))
                .collect(Collectors.toList());

        listener.onResult(
            ResolutionResult.newBuilder()
                .setAddresses(addrs)
                .setAttributes(Attributes.EMPTY)
                .build());
      }

      @Override
      public void shutdown() {}
    };
  }
}
