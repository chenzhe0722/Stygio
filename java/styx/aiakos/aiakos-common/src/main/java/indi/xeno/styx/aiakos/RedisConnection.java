package indi.xeno.styx.aiakos;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.resource.ClientResources;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static io.lettuce.core.internal.Futures.toCompletionStage;
import static io.lettuce.core.resource.ClientResources.builder;
import static java.util.stream.Collectors.toUnmodifiableList;

public class RedisConnection<K, V> {

  private final ClientResources resources;

  private final AbstractRedisClient client;

  private final RedisClusterAsyncCommands<K, V> cmd;

  public RedisConnection(
      Collection<String> uri, boolean cluster, RedisCodec<K, V> codec, boolean autoFlush) {
    this(builder().build(), uri, cluster, codec, autoFlush);
  }

  public RedisConnection(
      Collection<String> uri,
      boolean cluster,
      int ioThreadPoolSize,
      int computationThreadPoolSize,
      RedisCodec<K, V> codec,
      boolean autoFlush) {
    this(
        buildResources(ioThreadPoolSize, computationThreadPoolSize),
        uri,
        cluster,
        codec,
        autoFlush);
  }

  private RedisConnection(
      ClientResources resources,
      Collection<String> uri,
      boolean cluster,
      RedisCodec<K, V> codec,
      boolean autoFlush) {
    this.resources = resources;
    Stream<RedisURI> stream = uri.stream().filter(Objects::nonNull).map(RedisURI::create);
    if (cluster) {
      List<RedisURI> buildUri = stream.collect(toUnmodifiableList());
      if (buildUri.isEmpty()) {
        throw new IllegalArgumentException();
      }
      RedisClusterClient client = RedisClusterClient.create(resources, buildUri);
      this.client = client;
      cmd = setAutoFlush(client.connect(codec).async(), autoFlush);
    } else {
      RedisURI buildUri = stream.findFirst().orElseThrow();
      RedisClient client = RedisClient.create(resources);
      this.client = client;
      cmd = setAutoFlush(client.connect(codec, buildUri).async(), autoFlush);
    }
  }

  private static ClientResources buildResources(
      int ioThreadPoolSize, int computationThreadPoolSize) {
    return builder()
        .ioThreadPoolSize(ioThreadPoolSize)
        .computationThreadPoolSize(computationThreadPoolSize)
        .build();
  }

  private static <L, R> RedisClusterAsyncCommands<L, R> setAutoFlush(
      RedisClusterAsyncCommands<L, R> command, boolean autoFlush) {
    command.setAutoFlushCommands(autoFlush);
    return command;
  }

  public RedisClusterAsyncCommands<K, V> getCmd() {
    return cmd;
  }

  public CompletableFuture<Boolean> shutdown() {
    return client.shutdownAsync().thenCompose(ignore -> toCompletionStage(resources.shutdown()));
  }
}
