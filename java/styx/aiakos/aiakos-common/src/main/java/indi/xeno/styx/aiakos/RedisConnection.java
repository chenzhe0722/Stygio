package indi.xeno.styx.aiakos;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.reactive.RedisClusterReactiveCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.resource.ClientResources;

import java.util.concurrent.CompletableFuture;

import static indi.xeno.styx.aiakos.RedisType.CLUSTER;

public class RedisConnection<K, V> {

  private final AbstractRedisClient client;

  private final RedisClusterReactiveCommands<K, V> cmd;

  public RedisConnection(
      RedisType type, ClientResources resources, RedisURI uri, RedisCodec<K, V> codec) {
    if (CLUSTER.equals(type)) {
      RedisClusterClient client = RedisClusterClient.create(resources, uri);
      this.client = client;
      cmd = client.connect(codec).reactive();
    } else {
      RedisClient client = RedisClient.create(resources, uri);
      this.client = client;
      cmd = client.connect(codec).reactive();
    }
  }

  public CompletableFuture<Void> close() {
    return client.shutdownAsync();
  }
}
