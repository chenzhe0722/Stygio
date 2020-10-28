package indi.xeno.styx.thanatos.exec

import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands
import org.apache.spark.sql.Row

import java.nio.charset.Charset
import java.util.concurrent.CompletionStage
import java.util.{HashMap => JHashMap}

private[exec] object RedisOperator {

  def setOp(
      cmd: RedisClusterAsyncCommands[Array[Byte], Array[Byte]],
      keyCharset: Charset,
      valueFn: Row => Array[Byte]
  )(row: Row): CompletionStage[_] = {
    cmd.set(row.getString(0).getBytes(keyCharset), valueFn(row))
  }

  def hsetOp(
      cmd: RedisClusterAsyncCommands[Array[Byte], Array[Byte]],
      keyCharset: Charset,
      mapFn: Row => Iterator[(Array[Byte], Array[Byte])]
  )(row: Row): CompletionStage[_] = {
    val map = new JHashMap[Array[Byte], Array[Byte]]()
    mapFn(row).foreach(t => map.put(t._1, t._2))
    cmd.hset(row.getString(0).getBytes(keyCharset), map)
  }
}
